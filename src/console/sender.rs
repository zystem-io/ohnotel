use crate::collect::periodic;
use crate::console::dto::Metric;
use log::error;
use std::io;
use tokio::io::{AsyncWriteExt as _, stdout};

/// Sends console metrics to standard output as a multi-line text block per
/// metric (header line + indented data points).
#[derive(Debug, Default, Clone, Copy)]
pub struct Stdout;

impl periodic::Sender<Metric> for Stdout {
    async fn send(&mut self, metrics: Vec<Metric>) {
        if let Err(err) = write_metrics(metrics).await {
            error!("failed to write metrics to stdout: {err}");
        }
    }
}

async fn write_metrics(metrics: Vec<Metric>) -> io::Result<()> {
    let mut stdout = stdout();

    for metric in metrics {
        stdout.write_all(metric.render().as_bytes()).await?;
        stdout.write_all(b"\n").await?;
    }

    stdout.flush().await
}
