use anyhow::Result;
use tokio::time::Instant;
use uniffle_worker::proto::uniffle::shuffle_server_client::ShuffleServerClient;
use uniffle_worker::write_read_for_one_time;

// This is a general benchmark client for apache uniffle server,
// you could use different concurrency to test the performance

// todo: implement more options to control the concurrency and batch data size

#[tokio::main]
async fn main() -> Result<()> {
    let port = 21100;
    let host = "0.0.0.0";

    let timer = Instant::now();
    let client = ShuffleServerClient::connect(format!("http://{}:{}", host, port)).await?;
    write_read_for_one_time(client)
        .await
        .expect("failed to test write -> read.");

    println!("cost [{}] ms", timer.elapsed().as_millis());

    Ok(())
}
