use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use futures::{ready, Future};
use log::debug;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::hdfs::datanode::{BlockReader, BlockWriter};
use crate::hdfs::ec::EcSchema;
use crate::hdfs::protocol::NamenodeProtocol;
use crate::proto::hdfs;
use crate::{HdfsError, Result};

pub struct FileReader {
    status: hdfs::HdfsFileStatusProto,
    located_blocks: hdfs::LocatedBlocksProto,
    ec_schema: Option<EcSchema>,
    position: usize,
}

impl FileReader {
    pub(crate) fn new(
        status: hdfs::HdfsFileStatusProto,
        located_blocks: hdfs::LocatedBlocksProto,
        ec_schema: Option<EcSchema>,
    ) -> Self {
        Self {
            status,
            located_blocks,
            ec_schema,
            position: 0,
        }
    }

    pub fn file_length(&self) -> usize {
        self.status.length as usize
    }

    pub fn remaining(&self) -> usize {
        if self.position > self.status.length as usize {
            0
        } else {
            self.status.length as usize - self.position
        }
    }

    /// Read up to `len` bytes into a new [Bytes] object, advancing the internal position in the file.
    /// An empty [Bytes] object will be returned if the end of the file has been reached.
    pub async fn read(&mut self, len: usize) -> Result<Bytes> {
        if self.position >= self.file_length() {
            Ok(Bytes::new())
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + len, self.file_length());
            self.read_range(offset, self.position - offset as usize)
                .await
        }
    }

    /// Read up to `buf.len()` bytes into the provided slice, advancing the internal position in the file.
    /// Returns the number of bytes that were read, or 0 if the end of the file has been reached.
    pub async fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.position >= self.file_length() {
            Ok(0)
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + buf.len(), self.file_length());
            let read_bytes = self.position - offset;
            self.read_range_buf(buf, offset).await?;
            Ok(read_bytes)
        }
    }

    /// Read up to `len` bytes starting at `offset` into a new [Bytes] object. The returned buffer
    /// could be smaller than `len` if `offset + len` extends beyond the end of the file.
    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        let end = usize::min(self.file_length(), offset + len);
        if offset >= end {
            return Err(HdfsError::InvalidArgument(
                "Offset is past the end of the file".to_string(),
            ));
        }

        let buf_size = end - offset;
        let mut buf = BytesMut::zeroed(buf_size);
        self.read_range_buf(&mut buf, offset).await?;
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer. Buffer will be extended by the length of the file.
    pub async fn read_range_buf(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        let block_readers = self.create_block_readers(offset, buf.len());

        let mut futures = Vec::new();

        let mut remaining = buf;

        for reader in block_readers.iter() {
            debug!("Block reader: {:?}", reader);
            let (left, right) = remaining.split_at_mut(reader.len);
            futures.push(reader.read(left));
            remaining = right;
        }

        for future in join_all(futures).await.into_iter() {
            future?;
        }

        Ok(())
    }

    fn create_block_readers(&self, offset: usize, len: usize) -> Vec<BlockReader> {
        self.located_blocks
            .blocks
            .iter()
            .flat_map(|block| {
                let block_file_start = block.offset as usize;
                let block_file_end = block_file_start + block.b.num_bytes.unwrap() as usize;

                if block_file_start <= (offset + len) && block_file_end > offset {
                    // We need to read this block
                    let block_start = offset - usize::min(offset, block_file_start);
                    let block_end = usize::min(offset + len, block_file_end) - block_file_start;
                    Some(BlockReader::new(
                        block.clone(),
                        self.ec_schema.clone(),
                        block_start,
                        block_end - block_start,
                    ))
                } else {
                    // No data is needed from this block
                    None
                }
            })
            .collect()
    }
}

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + Sync + 'static>>;

enum FileWriterState {
    New,
    Ready(BlockWriter),
    LoadingBlock(BoxedTryFuture<BlockWriter>),
    ShuttingDown(BoxedTryFuture<()>),
    Error(String),
}

pub struct FileWriter {
    src: String,
    protocol: Arc<NamenodeProtocol>,
    status: hdfs::HdfsFileStatusProto,
    server_defaults: hdfs::FsServerDefaultsProto,
    state: FileWriterState,
    closed: bool,
    bytes_written: usize,
}

impl FileWriter {
    pub(crate) fn new(
        protocol: Arc<NamenodeProtocol>,
        src: String,
        status: hdfs::HdfsFileStatusProto,
        server_defaults: hdfs::FsServerDefaultsProto,
    ) -> Self {
        Self {
            protocol,
            src,
            status,
            server_defaults,
            state: FileWriterState::New,
            closed: false,
            bytes_written: 0,
        }
    }

    fn load_block_task(&self, prev: Option<&BlockWriter>) -> BoxedTryFuture<BlockWriter> {
        let protocol = Arc::clone(&self.protocol);
        let src = self.src.clone();
        let file_id = self.status.file_id.clone();
        let blocksize = self.status.blocksize() as usize;
        let server_defaults = self.server_defaults.clone();
        let extended_block = prev.map(|b| b.get_extended_block());
        Box::pin(async move {
            let new_block = protocol.add_block(&src, extended_block, file_id).await?;
            Ok(BlockWriter::new(new_block.block, blocksize, server_defaults).await?)
        })
    }

    fn poll_task(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        match &mut self.state {
            FileWriterState::Ready(block_writer) => {
                if block_writer.is_full() {
                    ready!(Pin::new(block_writer).poll_shutdown(cx))?;
                    let mut task = self.load_block_task(Some(block_writer));
                    // We need to immediately poll the task to register the context, and handle the case it immediately returns
                    match Pin::new(&mut task).poll(cx) {
                        Poll::Pending => {
                            self.state = FileWriterState::LoadingBlock(task);
                            Poll::Pending
                        }
                        Poll::Ready(new_writer) => {
                            self.state = FileWriterState::Ready(new_writer?);
                            Poll::Ready(Ok(()))
                        }
                    }
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            FileWriterState::LoadingBlock(task) => match Pin::new(task).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.state = match result {
                        Ok(block_writer) => FileWriterState::Ready(block_writer),
                        Err(e) => FileWriterState::Error(e.to_string()),
                    };
                    Poll::Ready(Ok(()))
                }
            },
            _ => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let this = self.get_mut();
        ready!(this.poll_task(cx));

        let mut bytes_written = 0usize;

        this.state = match core::mem::replace(
            &mut this.state,
            FileWriterState::Error("Error occured during write".to_string()),
        ) {
            FileWriterState::Ready(mut block_writer) => {
                match Pin::new(&mut block_writer).poll_write(cx, buf) {
                    Poll::Ready(written) => {
                        bytes_written = written?;
                    }
                    Poll::Pending => (),
                }
                FileWriterState::Ready(block_writer)
            }
            _ => FileWriterState::Error("FileWriter not ready for writes".to_string()),
        };

        if bytes_written > 0 {
            Poll::Ready(Ok(bytes_written))
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        // Push to the block writer if one currently exists, otherwise ignore
        let this = self.get_mut();

        match &mut this.state {
            FileWriterState::Ready(writer) => {
                Poll::Ready(Ok(ready!(Pin::new(writer).poll_flush(cx))?))
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        let this = self.get_mut();

        match &mut this.state {
            FileWriterState::New => {
                // No data was written, just complete the file
                let protocol = Arc::clone(&this.protocol);
                let task: BoxedTryFuture<()> = Box::pin(async move {
                    protocol
                        .complete(&self.src, None, self.status.file_id)
                        .await?;
                    Ok(())
                });
                this.state = FileWriterState::ShuttingDown(task)
            }
            FileWriterState::Ready(writer) => {
                ready!(Pin::new(writer).poll_shutdown(cx))?;

                let protocol = Arc::clone(&this.protocol);
                let extended_block = writer.get_extended_block();
                let task: BoxedTryFuture<()> = Box::pin(async move {
                    protocol
                        .complete(&self.src, Some(extended_block), self.status.file_id)
                        .await?;
                    Ok(())
                });
                this.state = FileWriterState::ShuttingDown(task)
            }
            FileWriterState::ShuttingDown(task) => {
                ready!(Pin::new(task).poll(cx))?;
            }

            _ => Err(HdfsError::DataTransferError(
                "File writer not in a state to shutdown".to_string(),
            ))?,
        }

        Poll::Ready(Ok(()))
    }
}
