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

//! An object store implementation for Google Cloud Storage **Zonal Buckets**
//! (Rapid storage class).
//!
//! Zonal Buckets support **appendable objects** – objects that can be opened,
//! written to incrementally, flushed, paused, resumed, and finalized.  These
//! operations are only available via the GCS gRPC API
//! (`google.storage.v2.Storage`), not REST.
//!
//! This module:
//!
//! * Uses gRPC (`BidiWriteObject` / `ReadObject`) for read / write operations.
//! * Delegates operations that do not need gRPC (delete, list, copy,
//!   multipart) to the inner [`GoogleCloudStorage`] HTTP client.
//! * Exposes appendable-object-specific operations via the [`AppendableStore`]
//!   trait.
//!
//! # Feature flag
//!
//! Enable the **`gcp_rapid`** feature in your `Cargo.toml`:
//!
//! ```toml
//! object_store = { version = "...", features = ["gcp_rapid"] }
//! ```
//!
//! # Example
//!
//! ```no_run
//! # use object_store::gcp_rapid::GoogleCloudStorageRapidBuilder;
//! # use object_store::ObjectStore;
//! # async fn example() -> object_store::Result<()> {
//! let store = GoogleCloudStorageRapidBuilder::from_env().build()?;
//! store.put(&"hello.txt".into(), "world".into()).await?;
//! let result = store.get(&"hello.txt".into()).await?;
//! let bytes = result.bytes().await?;
//! assert_eq!(&bytes[..], b"world");
//! # Ok(())
//! # }
//! ```
//!
//! [`GoogleCloudStorage`]: crate::gcp::GoogleCloudStorage

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;

use google_api_proto::google::storage::v2::{
    bidi_write_object_response, ChecksummedData, BidiWriteObjectResponse,
};

use std::collections::HashMap;
use std::ops::Range;

use crate::gcp::GoogleCloudStorage;
use crate::path::Path;
use crate::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};

mod builder;
pub(crate) mod client;

pub use builder::GoogleCloudStorageRapidBuilder;

use client::{
    map_grpc_error, BidiReadObjectRequest, BidiReadObjectResponse, BidiWriteObjectRequestExt,
    GrpcStorageClient, ReadRange, STORE, bidi_data_ext,
};
pub use client::BidiReadHandle;

// ---------------------------------------------------------------------------
// GoogleCloudStorageRapid
// ---------------------------------------------------------------------------

/// Object store backed by Google Cloud Storage **Zonal Buckets** (Rapid
/// storage class) using the gRPC Storage v2 API.
///
/// Build with [`GoogleCloudStorageRapidBuilder`].
#[derive(Debug, Clone)]
pub struct GoogleCloudStorageRapid {
    pub(crate) inner: GoogleCloudStorage,
    pub(crate) grpc_client: Arc<GrpcStorageClient>,
}

impl fmt::Display for GoogleCloudStorageRapid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GoogleCloudStorageRapid({})",
            self.inner.bucket_name()
        )
    }
}

// ---------------------------------------------------------------------------
// ObjectStore implementation – delegates non-gRPC ops to `inner`
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectStore for GoogleCloudStorageRapid {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Collect PutPayload chunks into a single Bytes
        let total_len = payload.content_length();
        let data: Bytes = if total_len == 0 {
            Bytes::new()
        } else {
            let mut buf = bytes::BytesMut::with_capacity(total_len);
            for chunk in &payload {
                buf.extend_from_slice(chunk);
            }
            buf.freeze()
        };

        self.grpc_client.put(location, data, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Delegate multipart to the inner HTTP client
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // Map GetRange to read_offset / read_limit
        let (read_offset, read_limit) = match &options.range {
            Some(GetRange::Bounded(r)) => (r.start as i64, (r.end - r.start) as i64),
            Some(GetRange::Offset(o)) => (*o as i64, 0),
            Some(GetRange::Suffix(_n)) => {
                // Suffix ranges need total size first; use offset=0 limit=0
                // and rely on server to return the last N bytes.
                // gRPC ReadObject doesn't directly support suffix ranges,
                // so we use a negative offset workaround:
                // read_offset = -(n as i64) is not supported by the proto.
                // Instead, read entire object – this is a best-effort fallback.
                (0i64, 0i64)
            }
            None => (0, 0),
        };

        let (meta, total_size, stream) =
            self.grpc_client.get(location, read_offset, read_limit).await?;

        // Check preconditions against metadata
        options.check_preconditions(&meta)?;

        // Compute actual range returned
        let range_start = if read_offset >= 0 {
            read_offset as u64
        } else {
            0
        };
        let range_end = if read_limit > 0 {
            std::cmp::min(range_start + read_limit as u64, total_size)
        } else {
            total_size
        };

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: range_start..range_end,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut reader = self.open_bidi_reader(location).await?;
        reader.read_ranges(ranges).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// AppendWriter – handle to an open BidiWriteObject stream
// ---------------------------------------------------------------------------

/// A writer for an appendable object on a GCS Zonal Bucket.
///
/// Obtained via [`AppendableStore::start_append`] or
/// [`AppendableStore::resume_append`].
///
/// Data written through this writer is buffered in the gRPC stream and only
/// persisted when [`flush`](Self::flush) is called.  Call
/// [`finalize`](Self::finalize) to close the object (making it immutable).
pub struct AppendWriter {
    sender: tokio::sync::mpsc::Sender<BidiWriteObjectRequestExt>,
    response_stream: tonic::Streaming<BidiWriteObjectResponse>,
    write_offset: i64,
}

impl fmt::Debug for AppendWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendWriter")
            .field("write_offset", &self.write_offset)
            .finish()
    }
}

impl AppendWriter {
    pub(crate) fn new(
        sender: tokio::sync::mpsc::Sender<BidiWriteObjectRequestExt>,
        response_stream: tonic::Streaming<BidiWriteObjectResponse>,
    ) -> Self {
        Self {
            sender,
            response_stream,
            write_offset: 0,
        }
    }

    /// Write data to the appendable object without flushing.
    ///
    /// The data is sent over the gRPC stream but is **not** persisted until
    /// [`flush`](Self::flush) is called.
    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        let len = data.len() as i64;
        let msg = BidiWriteObjectRequestExt {
            write_offset: self.write_offset,
            data: Some(bidi_data_ext::Data::ChecksummedData(ChecksummedData {
                content: data,
                crc32c: None,
            })),
            ..Default::default()
        };

        self.sender.send(msg).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        self.write_offset += len;
        Ok(())
    }

    /// Flush: persist all data written so far to durable storage.
    ///
    /// Sends a `flush = true` message and waits for the server to confirm the
    /// persisted size.
    pub async fn flush(&mut self) -> Result<i64> {
        let msg = BidiWriteObjectRequestExt {
            write_offset: self.write_offset,
            flush: true,
            state_lookup: true,
            ..Default::default()
        };

        self.sender.send(msg).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        // Read response to get persisted size
        let resp = self
            .response_stream
            .message()
            .await
            .map_err(|e| map_grpc_error(e, "flush"))?
            .ok_or_else(|| Error::Generic {
                store: STORE,
                source: "Stream closed during flush".into(),
            })?;

        match resp.write_status {
            Some(bidi_write_object_response::WriteStatus::PersistedSize(size)) => Ok(size),
            Some(bidi_write_object_response::WriteStatus::Resource(_)) => {
                // Object was finalized (unexpected during flush)
                Ok(self.write_offset)
            }
            None => Ok(self.write_offset),
        }
    }

    /// Finalize the object, making it immutable.
    ///
    /// After finalization, no more writes are possible.
    pub async fn finalize(mut self) -> Result<PutResult> {
        let msg = BidiWriteObjectRequestExt {
            write_offset: self.write_offset,
            finish_write: true,
            ..Default::default()
        };

        self.sender.send(msg).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        // Drop sender to close the request stream
        drop(self.sender);

        // Read final response
        let mut e_tag = None;
        let mut version = None;
        while let Some(resp) = self
            .response_stream
            .message()
            .await
            .map_err(|e| map_grpc_error(e, "finalize"))?
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

    /// Pause the append by dropping the stream without finalizing.
    ///
    /// The object remains appendable and can be resumed later with
    /// [`AppendableStore::resume_append`].
    pub fn pause(self) {
        // Dropping self closes sender + response_stream,
        // which terminates the gRPC stream without finish_write.
    }

    /// Returns the current write offset (total bytes sent, not necessarily
    /// persisted).
    pub fn write_offset(&self) -> i64 {
        self.write_offset
    }
}

// ---------------------------------------------------------------------------
// BidiReader – handle to an open BidiReadObject stream
// ---------------------------------------------------------------------------

/// A reader for efficient multi-range reads from a GCS object using the
/// `BidiReadObject` bidirectional streaming RPC.
///
/// Obtained via [`BidiReadableStore::open_bidi_reader`] or
/// [`BidiReadableStore::open_bidi_reader_with_handle`].
///
/// Submit one or more byte ranges with [`read_ranges`](Self::read_ranges) and
/// receive the corresponding data.  The server may return a
/// [`BidiReadHandle`] that can be used to open subsequent streams with
/// connection affinity.
pub struct BidiReader {
    sender: tokio::sync::mpsc::Sender<BidiReadObjectRequest>,
    response_stream: tonic::Streaming<BidiReadObjectResponse>,
    read_handle: Option<BidiReadHandle>,
    metadata: ObjectMeta,
    next_read_id: i64,
}

impl fmt::Debug for BidiReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BidiReader")
            .field("metadata", &self.metadata)
            .field("has_read_handle", &self.read_handle.is_some())
            .finish()
    }
}

impl BidiReader {
    pub(crate) async fn new(
        sender: tokio::sync::mpsc::Sender<BidiReadObjectRequest>,
        mut response_stream: tonic::Streaming<BidiReadObjectResponse>,
        location: &Path,
    ) -> Result<Self> {
        // The first response contains object metadata
        let first = response_stream
            .message()
            .await
            .map_err(|e| map_grpc_error(e, location.as_ref()))?
            .ok_or_else(|| Error::Generic {
                store: STORE,
                source: "Empty response stream from BidiReadObject".into(),
            })?;

        let obj = first.metadata.ok_or_else(|| Error::Generic {
            store: STORE,
            source: "No metadata in first BidiReadObject response".into(),
        })?;

        let metadata = client::object_to_meta(location, &obj)?;
        let read_handle = first.read_handle;

        Ok(Self {
            sender,
            response_stream,
            read_handle,
            metadata,
            next_read_id: 0,
        })
    }

    /// Object metadata obtained when the stream was opened.
    pub fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    /// Read multiple byte ranges from the object in a single round-trip.
    ///
    /// Returns one `Bytes` per input range, in the same order.
    pub async fn read_ranges(&mut self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let base_id = self.next_read_id;
        let read_ranges: Vec<ReadRange> = ranges
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let len = r.end.saturating_sub(r.start);
                ReadRange {
                    read_offset: r.start as i64,
                    read_length: len as i64,
                    read_id: base_id + i as i64,
                }
            })
            .collect();
        self.next_read_id = base_id + ranges.len() as i64;

        let request = BidiReadObjectRequest {
            read_object_spec: None,
            read_ranges,
        };
        self.sender.send(request).await.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;

        // Accumulate data per read_id
        let mut buffers: HashMap<i64, bytes::BytesMut> = HashMap::new();
        let mut completed: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let expected = ranges.len();

        while completed.len() < expected {
            let resp = self
                .response_stream
                .message()
                .await
                .map_err(|e| map_grpc_error(e, "bidi_read"))?
                .ok_or_else(|| Error::Generic {
                    store: STORE,
                    source: "Stream closed before all ranges were read".into(),
                })?;

            // Update read handle if refreshed
            if let Some(handle) = resp.read_handle {
                self.read_handle = Some(handle);
            }

            for range_data in resp.object_data_ranges {
                let read_id = range_data
                    .read_range
                    .as_ref()
                    .map(|r| r.read_id)
                    .unwrap_or(base_id);

                if let Some(cd) = range_data.checksummed_data {
                    if !cd.content.is_empty() {
                        buffers
                            .entry(read_id)
                            .or_default()
                            .extend_from_slice(&cd.content);
                    }
                }

                if range_data.range_end {
                    completed.insert(read_id);
                }
            }
        }

        // Return results in the same order as the input ranges
        let results = (0..ranges.len() as i64)
            .map(|i| {
                let id = base_id + i;
                buffers
                    .remove(&id)
                    .map(|b| b.freeze())
                    .unwrap_or_default()
            })
            .collect();

        Ok(results)
    }

    /// Read a single byte range from the object.
    pub async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        let mut results = self.read_ranges(&[range]).await?;
        Ok(results.remove(0))
    }

    /// Return the current read handle, if any.
    ///
    /// This handle can be passed to
    /// [`BidiReadableStore::open_bidi_reader_with_handle`] to open a new
    /// stream with connection affinity to the same server.
    pub fn read_handle(&self) -> Option<&BidiReadHandle> {
        self.read_handle.as_ref()
    }

    /// Close the reader, dropping the underlying gRPC stream.
    pub fn close(self) {
        // Dropping self closes sender + response_stream
    }
}

// ---------------------------------------------------------------------------
// BidiReadableStore trait
// ---------------------------------------------------------------------------

/// Extension trait for object stores that support bidirectional streaming reads.
///
/// `BidiReadObject` enables efficient multi-range reads within a single gRPC
/// stream and read-handle reuse for connection affinity.  This is supported
/// on GCS Zonal Buckets with the Rapid storage class.
#[async_trait]
pub trait BidiReadableStore: ObjectStore {
    /// Open a bidirectional read stream to the given object.
    ///
    /// Returns a [`BidiReader`] that supports efficient multi-range reads
    /// and read handle reuse.
    async fn open_bidi_reader(&self, location: &Path) -> Result<BidiReader>;

    /// Open a bidirectional read stream reusing a previously obtained
    /// [`BidiReadHandle`] for connection affinity.
    async fn open_bidi_reader_with_handle(
        &self,
        location: &Path,
        read_handle: BidiReadHandle,
    ) -> Result<BidiReader>;
}

#[async_trait]
impl BidiReadableStore for GoogleCloudStorageRapid {
    async fn open_bidi_reader(&self, location: &Path) -> Result<BidiReader> {
        let (tx, stream) = self.grpc_client.bidi_read_object(location, None).await?;
        BidiReader::new(tx, stream, location).await
    }

    async fn open_bidi_reader_with_handle(
        &self,
        location: &Path,
        read_handle: BidiReadHandle,
    ) -> Result<BidiReader> {
        let (tx, stream) = self
            .grpc_client
            .bidi_read_object(location, Some(read_handle))
            .await?;
        BidiReader::new(tx, stream, location).await
    }
}

// ---------------------------------------------------------------------------
// AppendableStore trait
// ---------------------------------------------------------------------------

/// Extension trait for object stores that support appendable objects.
///
/// Appendable objects can be written to incrementally, flushed, paused,
/// resumed, and finalized.  This is supported on GCS Zonal Buckets with
/// the Rapid storage class.
#[async_trait]
pub trait AppendableStore: ObjectStore {
    /// Create a new appendable object and return an [`AppendWriter`].
    ///
    /// The object is created in an open/appendable state.  Write data with
    /// [`AppendWriter::write`], persist with [`AppendWriter::flush`], and
    /// close with [`AppendWriter::finalize`].
    async fn start_append(&self, location: &Path) -> Result<AppendWriter>;

    /// Resume appending to an existing appendable object.
    ///
    /// `generation` identifies the specific version of the object to resume.
    async fn resume_append(&self, location: &Path, generation: i64) -> Result<AppendWriter>;

    /// Read from an object starting at the given byte offset.
    ///
    /// This is useful for "tailing" an appendable object that is still being
    /// written to by another process.
    async fn tail_read(&self, location: &Path, offset: u64) -> Result<GetResult>;
}

#[async_trait]
impl AppendableStore for GoogleCloudStorageRapid {
    async fn start_append(&self, location: &Path) -> Result<AppendWriter> {
        let (tx, response_stream) = self.grpc_client.start_append(location).await?;
        Ok(AppendWriter::new(tx, response_stream))
    }

    async fn resume_append(&self, location: &Path, generation: i64) -> Result<AppendWriter> {
        let (tx, response_stream) =
            self.grpc_client.resume_append(location, generation).await?;
        Ok(AppendWriter::new(tx, response_stream))
    }

    async fn tail_read(&self, location: &Path, offset: u64) -> Result<GetResult> {
        let (meta, total_size, stream) =
            self.grpc_client.get(location, offset as i64, 0).await?;

        let range_start = offset;
        let range_end = total_size;

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: range_start..range_end,
            attributes: Default::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_put_get_roundtrip() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/roundtrip.txt");
        let data = PutPayload::from("hello rapid");

        store.put(&path, data).await.expect("put failed");

        let result = store.get(&path).await.expect("get failed");
        let bytes = result.bytes().await.expect("bytes failed");
        assert_eq!(&bytes[..], b"hello rapid");

        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_append_flush_finalize() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/appendable.txt");

        let mut writer = store.start_append(&path).await.expect("start_append failed");

        writer
            .write(Bytes::from("chunk1"))
            .await
            .expect("write failed");
        writer
            .write(Bytes::from("chunk2"))
            .await
            .expect("write failed");

        let persisted = writer.flush().await.expect("flush failed");
        assert!(persisted > 0, "expected positive persisted size");

        let result = writer.finalize().await.expect("finalize failed");
        assert!(result.version.is_some());

        // Read back the full object
        let get_result = store.get(&path).await.expect("get failed");
        let bytes = get_result.bytes().await.expect("bytes failed");
        assert_eq!(&bytes[..], b"chunk1chunk2");

        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_tail_read() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/tail.txt");
        let data = PutPayload::from("abcdefghij");
        store.put(&path, data).await.expect("put failed");

        let result = store.tail_read(&path, 5).await.expect("tail_read failed");
        let bytes = result.bytes().await.expect("bytes failed");
        assert_eq!(&bytes[..], b"fghij");

        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_bidi_read_single_range() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/bidi_single.txt");
        let data = PutPayload::from("hello bidi read");
        store.put(&path, data).await.expect("put failed");

        let mut reader = store
            .open_bidi_reader(&path)
            .await
            .expect("open_bidi_reader failed");

        let bytes = reader
            .read_range(0..5)
            .await
            .expect("read_range failed");
        assert_eq!(&bytes[..], b"hello");

        reader.close();
        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_bidi_read_multi_range() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/bidi_multi.txt");
        let data = PutPayload::from("aaa-bbb-ccc-ddd");
        store.put(&path, data).await.expect("put failed");

        let mut reader = store
            .open_bidi_reader(&path)
            .await
            .expect("open_bidi_reader failed");

        let ranges = vec![0..3, 4..7, 12..15];
        let results = reader
            .read_ranges(&ranges)
            .await
            .expect("read_ranges failed");

        assert_eq!(results.len(), 3);
        assert_eq!(&results[0][..], b"aaa");
        assert_eq!(&results[1][..], b"bbb");
        assert_eq!(&results[2][..], b"ddd");

        reader.close();
        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_bidi_read_handle_reuse() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/bidi_handle.txt");
        let data = PutPayload::from("handle reuse test");
        store.put(&path, data).await.expect("put failed");

        // First reader: read and obtain handle
        let mut reader = store
            .open_bidi_reader(&path)
            .await
            .expect("open_bidi_reader failed");

        let bytes = reader
            .read_range(0..6)
            .await
            .expect("read_range failed");
        assert_eq!(&bytes[..], b"handle");

        let handle = reader.read_handle().cloned();
        reader.close();

        // Second reader: reuse handle if available
        if let Some(h) = handle {
            let mut reader2 = store
                .open_bidi_reader_with_handle(&path, h)
                .await
                .expect("open_bidi_reader_with_handle failed");

            let bytes2 = reader2
                .read_range(7..12)
                .await
                .expect("read_range failed");
            assert_eq!(&bytes2[..], b"reuse");
            reader2.close();
        }

        store.delete(&path).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires GCS zonal bucket credentials"]
    async fn rapid_get_ranges_via_bidi() {
        let store = GoogleCloudStorageRapidBuilder::from_env()
            .build()
            .expect("failed to build rapid store");

        let path = Path::from("rapid_test/get_ranges.txt");
        let data = PutPayload::from("0123456789abcdef");
        store.put(&path, data).await.expect("put failed");

        let ranges = vec![0..4, 10..14];
        let results = store
            .get_ranges(&path, &ranges)
            .await
            .expect("get_ranges failed");

        assert_eq!(results.len(), 2);
        assert_eq!(&results[0][..], b"0123");
        assert_eq!(&results[1][..], b"abcd");

        store.delete(&path).await.expect("delete failed");
    }
}
