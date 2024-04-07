use std::collections::HashSet;

use bytes::{Buf, BufMut, BytesMut};
use criterion::*;
use hdfs::hdfs::get_hdfs;
use hdfs_native::{
    minidfs::{DfsFeatures, MiniDfs},
    Client, WriteOptions,
};
use tokio::runtime::Runtime;

const INTS_TO_WRITE: usize = 32 * 1024 * 1024; // 128 MiB file

async fn write_file(client: &Client, path: &str, ints: usize) {
    let mut writer = client
        .create(path, WriteOptions::default().overwrite(true))
        .await
        .unwrap();

    let mut data = BytesMut::with_capacity(ints * 4);
    for i in 0..ints {
        data.put_u32(i as u32);
    }
    writer.write(data.freeze()).await.unwrap();
    writer.close().await.unwrap();
}

fn bench_read(c: &mut Criterion, rt: &Runtime, name: &str, filename: &str) {
    let client = Client::default();

    let mut group = c.benchmark_group(format!("read-{}", name));
    group.throughput(Throughput::Bytes((INTS_TO_WRITE * 4) as u64));
    group.sample_size(10);

    group.bench_function("read-native", |b| {
        rt.block_on(async { write_file(&client, filename, INTS_TO_WRITE).await });
        b.to_async(rt).iter(|| async {
            let reader = client.read(filename).await.unwrap();
            reader.read_range(0, reader.file_length()).await.unwrap()
        })
    });
    group.sample_size(10);
    group.bench_function("read-libhdfs", |b| {
        rt.block_on(async { write_file(&client, filename, INTS_TO_WRITE).await });
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = BytesMut::zeroed(INTS_TO_WRITE * 4);
            let mut bytes_read = 0;
            let reader = fs.open(filename).unwrap();

            while bytes_read < INTS_TO_WRITE * 4 {
                bytes_read += reader
                    .read(&mut buf[bytes_read..INTS_TO_WRITE * 4])
                    .unwrap() as usize;
            }
            reader.close().unwrap();
            buf
        })
    });
}

fn bench_write(c: &mut Criterion, rt: &Runtime, name: &str, filename: &str) {
    let client = Client::default();

    let mut data_to_write = BytesMut::with_capacity(INTS_TO_WRITE * 4);
    for i in 0..INTS_TO_WRITE {
        data_to_write.put_i32(i as i32);
    }

    let buf = data_to_write.freeze();

    let mut group = c.benchmark_group(format!("write-{}", name));
    group.throughput(Throughput::Bytes((INTS_TO_WRITE * 4) as u64));
    group.sample_size(10);
    group.bench_function("write-native", |b| {
        b.to_async(rt).iter(|| async {
            let mut writer = client
                .create(filename, WriteOptions::default().overwrite(true))
                .await
                .unwrap();

            writer.write(buf.clone()).await.unwrap();
            writer.close().await.unwrap();
        })
    });
    group.sample_size(10);
    group.bench_function("write-libhdfs", |b| {
        let fs = get_hdfs().unwrap();
        b.iter(|| {
            let mut buf = buf.clone();
            let writer = fs.create_with_overwrite(filename, true).unwrap();

            while buf.remaining() > 0 {
                let written = writer.write(&buf[..]).unwrap();
                buf.advance(written as usize);
            }
            writer.close().unwrap();
        })
    });
}

fn bench_with_features(
    c: &mut Criterion,
    rt: &Runtime,
    features: &HashSet<DfsFeatures>,
    name: &str,
    filename: &str,
) {
    let _dfs = MiniDfs::with_features(features);
    bench_read(c, rt, name, filename);
    bench_write(c, rt, name, filename);
}

fn bench(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    bench_with_features(c, &rt, &HashSet::new(), "simple", "/bench");
    bench_with_features(
        c,
        &rt,
        &HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::Token,
            DfsFeatures::AES,
        ]),
        "aes",
        "/bench",
    );
    bench_with_features(
        c,
        &rt,
        &HashSet::from([DfsFeatures::EC]),
        "aes",
        "/ec-3-2/bench",
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);
