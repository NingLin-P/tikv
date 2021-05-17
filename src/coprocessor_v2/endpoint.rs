// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::{CoprocessorPlugin, PluginError, RawResponse, Region, RegionEpoch};
use kvproto::coprocessor_v2 as coprv2pb;
use semver::VersionReq;
use std::future::Future;
use std::ops::Not;
use std::sync::Arc;

use super::config::Config;
use super::plugin_registry::PluginRegistry;
use super::raw_storage_impl::RawStorageImpl;
use crate::storage::{self, lock_manager::LockManager, Engine, Storage};

enum CoprocessorError {
    RegionError(kvproto::errorpb::Error),
    Other(String),
}

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint {
    plugin_registry: Arc<PluginRegistry>,
}

impl tikv_util::AssertSend for Endpoint {}

impl Endpoint {
    pub fn new(copr_cfg: &Config) -> Self {
        let mut plugin_registry = PluginRegistry::new();

        // Enable hot-reloading of plugins if the user configured a directory.
        if let Some(plugin_directory) = &copr_cfg.coprocessor_plugin_directory {
            let r = plugin_registry.start_hot_reloading(plugin_directory);
            if let Err(err) = r {
                warn!("unable to start hot-reloading for coprocessor plugins."; "coprocessor_directory" => plugin_directory.display(), "error" => ?err);
            }
        }

        Self {
            plugin_registry: Arc::new(plugin_registry),
        }
    }

    /// Handles a request to the coprocessor framework.
    ///
    /// Each request is dispatched to the corresponding coprocessor plugin based on it's `copr_name`
    /// field. A plugin with a matching name must be loaded by TiKV, otherwise an error is returned.
    #[inline]
    pub fn handle_request<E: Engine, L: LockManager>(
        &self,
        storage: &Storage<E, L>,
        req: coprv2pb::RawCoprocessorRequest,
    ) -> impl Future<Output = coprv2pb::RawCoprocessorResponse> {
        let mut response = coprv2pb::RawCoprocessorResponse::default();

        let coprocessor_result = self.handle_request_impl(storage, req);

        match coprocessor_result {
            Ok(data) => response.set_data(data),
            Err(CoprocessorError::RegionError(region_err)) => response.set_region_error(region_err),
            Err(CoprocessorError::Other(o)) => response.set_other_error(o),
        }

        std::future::ready(response)
    }

    #[inline]
    fn handle_request_impl<E: Engine, L: LockManager>(
        &self,
        storage: &Storage<E, L>,
        req: coprv2pb::RawCoprocessorRequest,
    ) -> Result<RawResponse, CoprocessorError> {
        let plugin = self
            .plugin_registry
            .get_plugin(&req.copr_name)
            .ok_or_else(|| {
                CoprocessorError::Other(format!(
                    "No registered coprocessor with name '{}'",
                    req.copr_name
                ))
            })?;

        // Check whether the found plugin satisfies the version constraint.
        let version_req = VersionReq::parse(&req.copr_version_constraint)
            .map_err(|e| CoprocessorError::Other(format!("{}", e)))?;
        let plugin_version = plugin.version();
        version_req
            .matches(&plugin_version)
            .not()
            .then(|| {})
            .ok_or_else(|| {
                CoprocessorError::Other(format!(
                    "The plugin '{}' with version '{}' does not satisfy the version constraint '{}'",
                    plugin.name(),
                    plugin_version,
                    version_req,
                ))
            })?;

        let raw_storage_api = RawStorageImpl::new(req.get_context(), storage);
        let region = Region {
            id: req.get_context().get_region_id(),
            region_epoch: RegionEpoch {
                conf_ver: req.get_context().get_region_epoch().get_conf_ver(),
                version: req.get_context().get_region_epoch().get_version(),
            },
        };

        let plugin_result = plugin.on_raw_coprocessor_request(&region, &req.data, &raw_storage_api);

        plugin_result.map_err(|err| {
            if let Some(region_err) = extract_region_error(&err) {
                CoprocessorError::RegionError(region_err)
            } else {
                CoprocessorError::Other(format!("{}", err))
            }
        })
    }
}

fn extract_region_error(error: &PluginError) -> Option<kvproto::errorpb::Error> {
    match error {
        PluginError::StorageError(storage_err) => match storage_err {
            coprocessor_plugin_api::StorageError::Other(other_err) => other_err
                .downcast_ref::<storage::Result<()>>()
                .and_then(|e| storage::errors::extract_region_error::<()>(e)),
            _ => None,
        },
    }
}
