use deltalake_test::utils::*;
use hdfs_native::{minidfs::MiniDfs, Client};
use hdfs_native_deltalake::register_handlers;
use std::{collections::HashSet, process::ExitStatus};

/// Kinds of storage integration
#[allow(dead_code)]
pub struct HdfsIntegration {
    minidfs: MiniDfs,
    client: Client,
}

impl Default for HdfsIntegration {
    fn default() -> Self {
        println!("Running default");
        register_handlers(None);
        let minidfs = MiniDfs::with_features(&HashSet::new());
        let client = Client::new(&minidfs.url).unwrap();
        println!("Finished, returning");
        Self { minidfs, client }
    }
}

impl StorageIntegration for HdfsIntegration {
    fn prepare_env(&self) {
        println!("Preparing env");
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        println!("Creating bucket");
        futures::executor::block_on(async {
            self.client
                .mkdirs(self.bucket_name().as_ref(), 0o755, true)
                .await
        })
        .unwrap();
        println!("Created bucket");

        Ok(ExitStatus::default())
    }

    fn bucket_name(&self) -> String {
        "/test-deltalake".to_string()
    }

    fn root_uri(&self) -> String {
        format!("hdfs://{}{}", self.minidfs.url, self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        todo!()
    }
}
