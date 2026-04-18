// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Builder for [`GoogleCloudStorageRapid`](super::GoogleCloudStorageRapid)

use std::sync::Arc;

use crate::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use crate::Result;

use super::client::{GrpcStorageClient, DEFAULT_GRPC_ENDPOINT};
use super::GoogleCloudStorageRapid;

/// Builder for [`GoogleCloudStorageRapid`].
///
/// Wraps a [`GoogleCloudStorageBuilder`] (or a pre-built [`GoogleCloudStorage`])
/// and adds the gRPC endpoint configuration needed for Rapid / Zonal Bucket
/// operations.
///
/// # Example
///
/// ```no_run
/// # use object_store::gcp_rapid::GoogleCloudStorageRapidBuilder;
/// let store = GoogleCloudStorageRapidBuilder::new()
///     .with_bucket_name("my-zonal-bucket")
///     .with_service_account_key("{...}")
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct GoogleCloudStorageRapidBuilder {
    /// Pre-built inner store – mutually exclusive with `inner_builder`.
    inner_store: Option<GoogleCloudStorage>,
    /// Builder for the inner store.
    inner_builder: GoogleCloudStorageBuilder,
    /// gRPC endpoint override (default: `https://storage.googleapis.com`).
    grpc_endpoint: Option<String>,
}

impl Default for GoogleCloudStorageRapidBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GoogleCloudStorageRapidBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            inner_store: None,
            inner_builder: GoogleCloudStorageBuilder::new(),
            grpc_endpoint: None,
        }
    }

    /// Create a builder pre-populated from environment variables.
    ///
    /// Delegates to [`GoogleCloudStorageBuilder::from_env`] for credential
    /// and bucket configuration.  The gRPC endpoint can be overridden via
    /// `GCS_GRPC_ENDPOINT`.
    pub fn from_env() -> Self {
        let grpc_endpoint = std::env::var("GCS_GRPC_ENDPOINT").ok();
        Self {
            inner_store: None,
            inner_builder: GoogleCloudStorageBuilder::from_env(),
            grpc_endpoint,
        }
    }

    /// Use an already-built [`GoogleCloudStorage`] for delegation.
    ///
    /// This reuses all credentials and HTTP configuration from the existing
    /// store.  The bucket name is extracted from the inner store.
    pub fn with_inner_store(mut self, store: GoogleCloudStorage) -> Self {
        self.inner_store = Some(store);
        self
    }

    /// Set the GCS bucket name.
    pub fn with_bucket_name(mut self, name: impl Into<String>) -> Self {
        self.inner_builder = self.inner_builder.with_bucket_name(name);
        self
    }

    /// Set the service account key (JSON string).
    pub fn with_service_account_key(mut self, key: impl Into<String>) -> Self {
        self.inner_builder = self.inner_builder.with_service_account_key(key);
        self
    }

    /// Set the path to a service account key file.
    pub fn with_service_account_path(mut self, path: impl Into<String>) -> Self {
        self.inner_builder = self.inner_builder.with_service_account_path(path);
        self
    }

    /// Set the application credentials path.
    pub fn with_application_credentials(mut self, path: impl Into<String>) -> Self {
        self.inner_builder = self.inner_builder.with_application_credentials(path);
        self
    }

    /// Override the gRPC endpoint.
    ///
    /// Defaults to `https://storage.googleapis.com`.
    pub fn with_grpc_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.grpc_endpoint = Some(endpoint.into());
        self
    }

    /// Build the [`GoogleCloudStorageRapid`] store.
    ///
    /// This will:
    /// 1. Build (or reuse) the inner [`GoogleCloudStorage`] for delegation.
    /// 2. Extract the credential provider from it.
    /// 3. Create a lazy tonic `Channel` to the gRPC endpoint.
    pub fn build(self) -> Result<GoogleCloudStorageRapid> {
        // 1. Inner store
        let inner = match self.inner_store {
            Some(s) => s,
            None => self.inner_builder.build()?,
        };

        // 2. Credentials + bucket from inner store
        let credentials = inner.credentials().clone();
        let bucket = inner.bucket_name().to_string();

        // 3. gRPC client
        let endpoint = self
            .grpc_endpoint
            .as_deref()
            .unwrap_or(DEFAULT_GRPC_ENDPOINT);

        let grpc_client = GrpcStorageClient::new(endpoint, credentials, bucket)?;

        Ok(GoogleCloudStorageRapid {
            inner,
            grpc_client: Arc::new(grpc_client),
        })
    }
}
