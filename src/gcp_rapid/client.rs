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

//! gRPC client wrapper for GCS Storage v2 API

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use prost::Message;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{metadata::MetadataValue, Request, Status};

use google_api_proto::google::storage::v2::{
    bidi_write_object_request, bidi_write_object_response, storage_client::StorageClient,
    BidiWriteObjectRequest, BidiWriteObjectResponse, ChecksummedData, Object, ObjectChecksums,
    ReadObjectRequest, ReadObjectResponse, WriteObjectSpec,
};

use crate::gcp::GcpCredentialProvider;
use crate::path::Path;
use crate::{Error, ObjectMeta, PutMode, PutOptions, PutResult, Result};

pub(crate) const STORE: &str = "GCSRapid";
pub(crate) const DEFAULT_GRPC_ENDPOINT: &str = "https://storage.googleapis.com";

// ---------------------------------------------------------------------------
// Compatibility shims for proto types not yet in google-api-proto 1.710
// ---------------------------------------------------------------------------

/// `AppendObjectSpec` – describes an attempt to append to an existing object.
///
/// Field numbers match the upstream `google/storage/v2/storage.proto`.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct AppendObjectSpec {
    /// `projects/{project}/buckets/{bucket}`
    #[prost(string, tag = "1")]
    pub bucket: String,
    /// Object name
    #[prost(string, tag = "2")]
    pub object: String,
    /// Generation of the object to append to
    #[prost(int64, tag = "3")]
    pub generation: i64,
    #[prost(int64, optional, tag = "4")]
    pub if_metageneration_match: Option<i64>,
    #[prost(int64, optional, tag = "5")]
    pub if_metageneration_not_match: Option<i64>,
    /// Opaque routing token
    #[prost(string, tag = "6")]
    pub routing_token: String,
    /// Server-provided resume handle
    #[prost(bytes = "vec", tag = "7")]
    pub write_handle: Vec<u8>,
}

/// `WriteObjectSpec` extended with the `appendable` field (tag 11).
#[derive(Clone, PartialEq, Message)]
pub(crate) struct WriteObjectSpecAppendable {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Object>,
    #[prost(string, tag = "7")]
    pub predefined_acl: String,
    #[prost(int64, optional, tag = "3")]
    pub if_generation_match: Option<i64>,
    #[prost(int64, optional, tag = "4")]
    pub if_generation_not_match: Option<i64>,
    #[prost(int64, optional, tag = "5")]
    pub if_metageneration_match: Option<i64>,
    #[prost(int64, optional, tag = "6")]
    pub if_metageneration_not_match: Option<i64>,
    #[prost(int64, optional, tag = "8")]
    pub object_size: Option<i64>,
    /// If `true`, the object remains appendable after the write completes.
    #[prost(bool, tag = "11")]
    pub appendable: bool,
}

/// Extended `BidiWriteObjectRequest` that includes the `AppendObjectSpec` oneof variant
/// and uses `WriteObjectSpecAppendable`.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct BidiWriteObjectRequestExt {
    #[prost(int64, tag = "3")]
    pub write_offset: i64,
    #[prost(message, optional, tag = "6")]
    pub object_checksums: Option<ObjectChecksums>,
    #[prost(bool, tag = "7")]
    pub state_lookup: bool,
    #[prost(bool, tag = "8")]
    pub flush: bool,
    #[prost(bool, tag = "9")]
    pub finish_write: bool,
    #[prost(oneof = "bidi_first_message_ext::FirstMessage", tags = "1, 2, 14")]
    pub first_message: Option<bidi_first_message_ext::FirstMessage>,
    #[prost(oneof = "bidi_data_ext::Data", tags = "4")]
    pub data: Option<bidi_data_ext::Data>,
}

pub(crate) mod bidi_first_message_ext {
    use super::*;

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(crate) enum FirstMessage {
        #[prost(string, tag = "1")]
        UploadId(String),
        #[prost(message, tag = "2")]
        WriteObjectSpec(WriteObjectSpecAppendable),
        #[prost(message, tag = "14")]
        AppendObjectSpec(AppendObjectSpec),
    }
}

pub(crate) mod bidi_data_ext {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(crate) enum Data {
        #[prost(message, tag = "4")]
        ChecksummedData(google_api_proto::google::storage::v2::ChecksummedData),
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

pub(crate) fn map_grpc_error(status: Status, path: &str) -> Error {
    match status.code() {
        tonic::Code::NotFound => Error::NotFound {
            path: path.to_string(),
            source: Box::new(status),
        },
        tonic::Code::AlreadyExists => Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(status),
        },
        tonic::Code::FailedPrecondition | tonic::Code::Aborted => Error::Precondition {
            path: path.to_string(),
            source: Box::new(status),
        },
        tonic::Code::PermissionDenied => Error::PermissionDenied {
            path: path.to_string(),
            source: Box::new(status),
        },
        tonic::Code::Unauthenticated => Error::Unauthenticated {
            path: path.to_string(),
            source: Box::new(status),
        },
        _ => Error::Generic {
            store: STORE,
            source: Box::new(status),
        },
    }
}

// ---------------------------------------------------------------------------
// GrpcStorageClient
// ---------------------------------------------------------------------------

/// Thin wrapper around the generated `StorageClient` that handles
/// authentication and provides higher-level helpers for put / get / append.
#[derive(Debug, Clone)]
pub(crate) struct GrpcStorageClient {
    client: StorageClient<Channel>,
    channel: Channel,
    credentials: GcpCredentialProvider,
    bucket: String,
}

impl GrpcStorageClient {
    /// Create a new client.  Uses `connect_lazy` so this is **not** async.
    pub(crate) fn new(
        endpoint: &str,
        credentials: GcpCredentialProvider,
        bucket: String,
    ) -> Result<Self> {
        let tls_config = ClientTlsConfig::new();
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?
            .tls_config(tls_config)
            .map_err(|e| Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?
            .connect_lazy();

        let client = StorageClient::new(channel.clone());

        Ok(Self {
            client,
            channel,
            credentials,
            bucket,
        })
    }

    // -- helpers ----------------------------------------------------------

    async fn get_bearer_token(&self) -> Result<String> {
        let cred = self.credentials.get_credential().await?;
        Ok(cred.bearer.clone())
    }

    fn bucket_resource(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }

    fn inject_auth<T>(request: &mut Request<T>, token: &str) -> Result<()> {
        let bearer = format!("Bearer {token}");
        let meta_val = bearer.parse::<MetadataValue<tonic::metadata::Ascii>>().map_err(|e| {
            Error::Generic {
                store: STORE,
                source: Box::new(e),
            }
        })?;
        request.metadata_mut().insert("authorization", meta_val);
        Ok(())
    }

    // -- put (simple write) -----------------------------------------------

    /// Write `payload` as a complete object at `location`.
    pub(crate) async fn put(
        &self,
        location: &Path,
        payload: Bytes,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let token = self.get_bearer_token().await?;
        let mut client = self.client.clone();

        let mut spec = WriteObjectSpec {
            resource: Some(Object {
                name: location.to_string(),
                bucket: self.bucket_resource(),
                ..Default::default()
            }),
            ..Default::default()
        };

        // Map PutMode to generation preconditions
        match opts.mode {
            PutMode::Create => {
                spec.if_generation_match = Some(0);
            }
            PutMode::Update(ref v) => {
                if let Some(ref ver) = v.version {
                    if let Ok(generation) = ver.parse::<i64>() {
                        spec.if_generation_match = Some(generation);
                    }
                }
            }
            PutMode::Overwrite => {}
        }

        let first_request = BidiWriteObjectRequest {
            first_message: Some(
                bidi_write_object_request::FirstMessage::WriteObjectSpec(spec),
            ),
            write_offset: 0,
            data: Some(bidi_write_object_request::Data::ChecksummedData(
                ChecksummedData {
                    content: payload,
                    crc32c: None,
                },
            )),
            finish_write: true,
            ..Default::default()
        };

        let stream = futures_util::stream::once(async { first_request });
        let mut req = Request::new(stream);
        Self::inject_auth(&mut req, &token)?;

        let response = client
            .bidi_write_object(req)
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?;

        let mut response_stream = response.into_inner();

        let mut e_tag = None;
        let mut version = None;
        while let Some(resp) = response_stream
            .message()
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?
        {
            if let Some(bidi_write_object_response::WriteStatus::Resource(obj)) =
                resp.write_status
            {
                if !obj.etag.is_empty() {
                    e_tag = Some(obj.etag);
                }
                version = Some(obj.generation.to_string());
            }
        }

        Ok(PutResult { e_tag, version })
    }

    // -- get (read) -------------------------------------------------------

    /// Read an object, returning metadata and a byte stream.
    pub(crate) async fn get(
        &self,
        location: &Path,
        read_offset: i64,
        read_limit: i64,
    ) -> Result<(ObjectMeta, u64, BoxStream<'static, Result<Bytes>>)> {
        let token = self.get_bearer_token().await?;
        let mut client = self.client.clone();

        let grpc_request = ReadObjectRequest {
            bucket: self.bucket_resource(),
            object: location.to_string(),
            read_offset,
            read_limit,
            ..Default::default()
        };

        let mut req = Request::new(grpc_request);
        Self::inject_auth(&mut req, &token)?;

        let response = client
            .read_object(req)
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?;

        let mut stream = response.into_inner();

        // The first message contains object metadata
        let first_msg = stream
            .message()
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?
            .ok_or_else(|| Error::Generic {
                store: STORE,
                source: "Empty response stream from ReadObject".into(),
            })?;

        let obj = first_msg.metadata.ok_or_else(|| Error::Generic {
            store: STORE,
            source: "No metadata in first ReadObject response".into(),
        })?;

        let meta = object_to_meta(location, &obj)?;
        let total_size = obj.size as u64;

        // Build byte stream: first message data + remaining messages
        let first_bytes = first_msg.checksummed_data.map(|cd| cd.content);
        let path_string = location.to_string();

        let byte_stream = futures_util::stream::try_unfold(
            (Some(first_bytes), stream, path_string),
            |(first, mut stream, path)| async move {
                // Yield first message data on the first call
                if let Some(maybe_bytes) = first {
                    if let Some(bytes) = maybe_bytes {
                        if !bytes.is_empty() {
                            return Ok(Some((bytes, (None, stream, path))));
                        }
                    }
                    // First chunk was empty; fall through to read from stream
                    return read_next_chunk(&mut stream, &path)
                        .await
                        .map(|opt| opt.map(|b| (b, (None, stream, path))));
                }

                read_next_chunk(&mut stream, &path)
                    .await
                    .map(|opt| opt.map(|b| (b, (None, stream, path))))
            },
        );

        Ok((meta, total_size, byte_stream.boxed()))
    }

    // -- append helpers (use extended proto types) -------------------------

    /// Open a new appendable object via `BidiWriteObject` with `appendable = true`.
    ///
    /// Returns the raw tonic response stream and an mpsc sender for subsequent
    /// write requests.  The caller (AppendWriter) owns both.
    pub(crate) async fn start_append(
        &self,
        location: &Path,
    ) -> Result<(
        tokio::sync::mpsc::Sender<BidiWriteObjectRequestExt>,
        tonic::Streaming<BidiWriteObjectResponse>,
    )> {
        let token = self.get_bearer_token().await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<BidiWriteObjectRequestExt>(8);

        // First message: WriteObjectSpecAppendable with appendable = true
        let first = BidiWriteObjectRequestExt {
            first_message: Some(bidi_first_message_ext::FirstMessage::WriteObjectSpec(
                WriteObjectSpecAppendable {
                    resource: Some(Object {
                        name: location.to_string(),
                        bucket: self.bucket_resource(),
                        ..Default::default()
                    }),
                    appendable: true,
                    ..Default::default()
                },
            )),
            state_lookup: true,
            ..Default::default()
        };
        tx.send(first).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        // Wrap receiver as a Stream
        let request_stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|msg| (msg, rx))
        });

        let mut req = Request::new(request_stream);
        Self::inject_auth(&mut req, &token)?;

        // Use raw tonic gRPC call because the generated client expects the non-extended type
        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;

        let codec = tonic::codec::ProstCodec::default();
        let path =
            http::uri::PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteObject");

        let response: tonic::Response<tonic::Streaming<BidiWriteObjectResponse>> = grpc
            .streaming(req, path, codec)
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?;

        Ok((tx, response.into_inner()))
    }

    /// Resume appending to an existing appendable object via `AppendObjectSpec`.
    pub(crate) async fn resume_append(
        &self,
        location: &Path,
        generation: i64,
    ) -> Result<(
        tokio::sync::mpsc::Sender<BidiWriteObjectRequestExt>,
        tonic::Streaming<BidiWriteObjectResponse>,
    )> {
        let token = self.get_bearer_token().await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<BidiWriteObjectRequestExt>(8);

        let first = BidiWriteObjectRequestExt {
            first_message: Some(bidi_first_message_ext::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket_resource(),
                    object: location.to_string(),
                    generation,
                    ..Default::default()
                },
            )),
            state_lookup: true,
            ..Default::default()
        };
        tx.send(first).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        let request_stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|msg| (msg, rx))
        });

        let mut req = Request::new(request_stream);
        Self::inject_auth(&mut req, &token)?;

        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;

        let codec = tonic::codec::ProstCodec::default();
        let path =
            http::uri::PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteObject");

        let response: tonic::Response<tonic::Streaming<BidiWriteObjectResponse>> = grpc
            .streaming(req, path, codec)
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?;

        Ok((tx, response.into_inner()))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read the next non-empty chunk from a `ReadObjectResponse` stream.
async fn read_next_chunk(
    stream: &mut tonic::Streaming<ReadObjectResponse>,
    path: &str,
) -> Result<Option<Bytes>> {
    loop {
        match stream.message().await {
            Ok(Some(msg)) => {
                if let Some(cd) = msg.checksummed_data {
                    if !cd.content.is_empty() {
                        return Ok(Some(cd.content));
                    }
                }
                // Empty chunk – continue reading
            }
            Ok(None) => return Ok(None),
            Err(e) => return Err(map_grpc_error(e, path)),
        }
    }
}

/// Convert a proto `Object` to an `ObjectMeta`.
fn object_to_meta(location: &Path, obj: &Object) -> Result<ObjectMeta> {
    let last_modified = obj
        .update_time
        .as_ref()
        .or(obj.create_time.as_ref())
        .map(|ts| {
            Utc.timestamp_opt(ts.seconds, ts.nanos as u32)
                .single()
                .unwrap_or_default()
        })
        .unwrap_or_default();

    Ok(ObjectMeta {
        location: location.clone(),
        last_modified,
        size: obj.size as u64,
        e_tag: if obj.etag.is_empty() {
            None
        } else {
            Some(obj.etag.clone())
        },
        version: Some(obj.generation.to_string()),
    })
}
