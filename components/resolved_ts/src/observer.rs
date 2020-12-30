// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

pub struct ChangeDataObserver<E: KvEngine> {
    cmd_batches: RefCell<Vec<CmdBatch>>,
    scheduler: Scheduler<Task<E::Snapshot>>,
}

impl<E: KvEngine> Clone for ChangeDataObserver<E> {
    fn clone(&self) -> ChangeDataObserver<E> {
        ChangeDataObserver {
            cmd_batches: self.cmd_batches.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

impl<E: KvEngine> ChangeDataObserver<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        ChangeDataObserver {
            cmd_batches: RefCell::default(),
            scheduler,
        }
    }

    pub fn register_to(self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }
}

impl<E: KvEngine> Coprocessor for ChangeDataObserver<E> {}

impl<E: KvEngine> CmdObserver<E> for ChangeDataObserver<E> {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, region_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            // let snapshot: ChangeDataSnapshot<E::Snapshot> = Box::new(engine.snapshot());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
            if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
                cmd_batch: batches,
                snapshot,
            }) {
                info!(""; "err" => ?e);
            }
        }
    }
}

impl<E: KvEngine> RoleObserver for ChangeDataObserver<E> {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if let Err(e) = self.scheduler.schedule(Task::RegionRoleChanged {
            role,
            region: ctx.region().clone(),
        }) {
            info!(""; "err" => ?e);
        }
    }
}

impl<E: KvEngine> RegionChangeObserver for ChangeDataObserver<E> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        match event {
            RegionChangeEvent::Destroy => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionDestroyed(ctx.region().clone()))
                {
                    info!(""; "err" => ?e);
                }
            }
            RegionChangeEvent::Update => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionUpdated(ctx.region().clone()))
                {
                    info!(""; "err" => ?e);
                }
            }
            RegionChangeEvent::Create => (),
        }
    }
}
