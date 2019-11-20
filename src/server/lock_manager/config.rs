// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::deadlock::Scheduler as DeadlockScheduler;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use crate::config::{ConfigManager, TiKvConfig};

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub wait_for_lock_timeout: u64,
    pub wake_up_delay_duration: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            wait_for_lock_timeout: 3000,
            wake_up_delay_duration: 1,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.wait_for_lock_timeout == 0 {
            return Err("pessimistic-txn.wait-for-lock-timeout can not be 0".into());
        }
        Ok(())
    }
}

pub struct ConfigMgr {
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: DeadlockScheduler,
}

impl ConfigMgr {
    pub fn new(
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: DeadlockScheduler,
    ) -> Self {
        ConfigMgr {
            waiter_mgr_scheduler,
            detector_scheduler,
        }
    }
}

impl ConfigManager for ConfigMgr {
    fn update(&mut self, incomming: &TiKvConfig) {
        let cfg = &incomming.pessimistic_txn;
        self.waiter_mgr_scheduler
            .change_config(cfg.wait_for_lock_timeout, cfg.wake_up_delay_duration);
        self.detector_scheduler
            .change_ttl(cfg.wait_for_lock_timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = Config::default();
        invalid_cfg.wait_for_lock_timeout = 0;
        assert!(invalid_cfg.validate().is_err());
    }
}
