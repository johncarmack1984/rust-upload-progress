use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::Region,
    operation::create_multipart_upload::CreateMultipartUploadOutput,
    types::{CompletedMultipartUpload, CompletedPart, StorageClass},
    Client as S3Client,
};
use aws_smithy_http::byte_stream::{ByteStream, Length};
// use aws_smithy_http::byte_stream::{ByteStream, Length};
use indicatif::{ProgressBar, ProgressStyle};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rust_upload_progress::TrackableBodyStream;
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

//In bytes, minimum chunk size of 5MB. Increase CHUNK_SIZE to send larger chunks.
const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
const MAX_CHUNKS: u64 = 10000;

pub async fn upload_file(
    aws_client: &S3Client,
    bucket_name: &str,
    key: &str,
) -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    let multipart_upload_res: CreateMultipartUploadOutput = aws_client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .storage_class(StorageClass::DeepArchive)
        .send()
        .await
        .unwrap();
    // snippet-end:[rust.example_code.s3.create_multipart_upload]
    let upload_id = multipart_upload_res.upload_id().unwrap();

    // //Create a file of random characters for the upload.
    // let mut file = File::create(&key).expect("Could not create sample file.");
    // // Loop until the file is 5 chunks.
    // let pb_local_write = ProgressBar::new(CHUNK_SIZE * 4);
    // pb_local_write.set_style(ProgressStyle::default_bar()
    //     .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.white/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
    //     .unwrap()
    //     .progress_chars("█  "));
    // let msg_local_write = format!("Creating sample file.");
    // pb_local_write.set_message(msg_local_write);
    // while file.metadata().unwrap().len() <= CHUNK_SIZE * 4 {
    //     let rand_string: String = thread_rng()
    //         .sample_iter(&Alphanumeric)
    //         .take(256)
    //         .map(char::from)
    //         .collect();
    //     let return_string: String = "\n".to_string();
    //     file.write_all(rand_string.as_ref())
    //         .expect("Error writing to file.");
    //     pb_local_write.set_position(file.metadata().unwrap().len());
    //     file.write_all(return_string.as_ref())
    //         .expect("Error writing to file.");
    //     pb_local_write.set_position(file.metadata().unwrap().len());
    // }
    // pb_local_write.finish_with_message("Done writing sample file.");
    // let mut file = File::open(key).unwrap();

    let path = Path::new(&key);
    let file_size = tokio::fs::metadata(path)
        .await
        .expect("it exists I swear")
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        panic!("Bad file size.");
    }
    if chunk_count > MAX_CHUNKS {
        panic!("Too many chunks! Try increasing your chunk size.")
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();

    println!("Uploading {} chunks.", chunk_count);

    let pb = ProgressBar::new(file_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.white/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .unwrap()
        .progress_chars("█  "));
    let msg = format!("Uploading {} to {}", key, bucket_name);
    pb.set_message(msg);

    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };
        let uploaded = chunk_index * CHUNK_SIZE;
        pb.set_message(format!(
            "Uploading chunk {} of {}.",
            chunk_index + 1,
            chunk_count
        ));
        // let mut body = TrackableBodyStream::try_from(PathBuf::from(key)).unwrap();
        // body.chunk_size(CHUNK_SIZE as usize);
        // body.offset(uploaded);
        // body.length(this_chunk);
        // println!("{}", body.content_length());
        // body.set_callback(move |tot_size: u64, sent: u64, cur_buf: u64| {
        // pb.inc(cur_buf as u64);
        // if sent == tot_size {
        //     pb.finish();
        // }
        // });h
        let stream = ByteStream::read_from()
            .path(path)
            .offset(uploaded)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .unwrap();
        //Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;
        // snippet-start:[rust.example_code.s3.upload_part]
        let upload_part_res = aws_client
            .upload_part()
            .key(key)
            .bucket(bucket_name)
            .upload_id(upload_id)
            .body(stream)
            // .body(stream.to_multipart_s3_stream())
            .part_number(part_number)
            .send()
            .await?;
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
        pb.set_position(uploaded + this_chunk);
        // snippet-end:[rust.example_code.s3.upload_part]
    }
    pb.finish_with_message("All chunks uploaded.");
    // snippet-start:[rust.example_code.s3.upload_part.CompletedMultipartUpload]
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();
    // snippet-end:[rust.example_code.s3.upload_part.CompletedMultipartUpload]
    println!("Completing upload.");
    // snippet-start:[rust.example_code.s3.complete_multipart_upload]
    let _complete_multipart_upload_res = aws_client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();
    // // snippet-end:[rust.example_code.s3.complete_multipart_upload]
    println!("Done uploading file.");

    return Ok(());
}

#[tokio::main]
async fn main() {
    let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"))
        .or_default_provider()
        .or_else("us-east-1");
    let sdk_config = aws_config::from_env().region(region_provider).load().await;
    let aws_client = S3Client::new(&sdk_config);
    let bucket_name = format!("s3-upload-test-jmc1984");
    let key = "sample.txt".to_string();
    aws_client
        .delete_object()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await
        .unwrap();

    println!("Object deleted.");
    upload_file(&aws_client, &bucket_name, &key).await.unwrap();
}
