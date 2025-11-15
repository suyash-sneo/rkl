mod args;
mod consumer;
mod merger;
mod models;
mod output;
mod query;
mod tui;

use anyhow::{Context, Result};
use args::{Cli, Commands, RunArgs};
use clap::Parser;
use colored::*;
use consumer::spawn_partition_consumer;
use merger::run_merger;
use models::{MessageEnvelope, OffsetSpec, SslConfig};
use output::TableOutput;
use query::{OrderDir, SelectItem, parse_query};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::io::Write as _;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_cli();
    let mode = std::env::var("RKL_MODE").unwrap_or_else(|_| "tui".to_string());
    match (mode.as_str(), cli.command) {
        ("tui", None) => {
            // TUI mode by default when no subcommand
            return tui::run(RunArgs::default()).await;
        }
        ("cli", None) => {
            // CLI mode without subcommand: parse RunArgs directly from argv
            let run_args = parse_runargs_from_argv();
            return run_once_cli(run_args).await;
        }
        (_, None) => {
            // Fallback to TUI for unknown mode
            return tui::run(RunArgs::default()).await;
        }
        (_, Some(Commands::Run(args))) => {
            let args = args;

            // Parse --query if provided and compute effective settings
            println!(
                "{}",
                format!("Connecting to Kafka broker: {}", args.broker).cyan()
            );
            let (query_ast, topic, columns, max_messages, order_desc) =
                if let Some(ref q) = args.query {
                    let ast = parse_query(q).context("Failed to parse --query")?;
                    let columns = ast.select.clone();
                    let max_messages = ast.limit.or(args.max_messages);
                    let order_desc = ast
                        .order
                        .as_ref()
                        .map(|o| matches!(o.dir, OrderDir::Desc))
                        .unwrap_or(false);
                    println!("{}", format!("Using query: {}", q).cyan());
                    println!("{}", format!("Topic: {}", ast.from).cyan());
                    let topic_name = ast.from.clone();
                    (Some(ast), topic_name, columns, max_messages, order_desc)
                } else {
                    let topic_value = args
                        .topic
                        .clone()
                        .expect("topic is required unless --query is provided");
                    println!("{}", format!("Topic: {}", topic_value).cyan());
                    let columns = SelectItem::standard(!args.keys_only);
                    (None, topic_value, columns, args.max_messages, false)
                };

            let keys_only = !columns.iter().any(|c| matches!(c, SelectItem::Value));

            // One-time consumer just to fetch metadata / partitions
            let mut probe_cfg = ClientConfig::new();
            probe_cfg
                .set("bootstrap.servers", &args.broker)
                .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .set("enable.partition.eof", "true");
            if args.ssl_ca_pem.is_some()
                || args.ssl_certificate_pem.is_some()
                || args.ssl_key_pem.is_some()
            {
                probe_cfg.set("security.protocol", "ssl");
                if let Some(ref s) = args.ssl_ca_pem {
                    probe_cfg.set("ssl.ca.pem", s);
                }
                if let Some(ref s) = args.ssl_certificate_pem {
                    probe_cfg.set("ssl.certificate.pem", s);
                }
                if let Some(ref s) = args.ssl_key_pem {
                    probe_cfg.set("ssl.key.pem", s);
                }
            }
            let probe_consumer: StreamConsumer = probe_cfg
                .create()
                .context("Failed to create probe consumer")?;

            let metadata = probe_consumer
                .fetch_metadata(Some(&topic), Duration::from_secs(10))
                .context("Failed to fetch metadata")?;

            let topic_md = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .context("Topic not found")?;

            let partitions: Vec<i32> = if let Some(p) = args.partition {
                vec![p]
            } else {
                topic_md.partitions().iter().map(|p| p.id()).collect()
            };

            println!(
                "{}",
                format!("Found {} partition(s): {:?}", partitions.len(), partitions).green()
            );
            println!("{}", "Starting readers (one per partition)...".yellow());

            // Message channel: producers = partition tasks, consumer = merger task
            let (tx, rx) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);

            // Spawn per-partition consumers
            let mut joinset = JoinSet::new();
            let offset_spec =
                OffsetSpec::from_str(&args.offset).unwrap_or_else(|_| OffsetSpec::Beginning);
            let query_arc = query_ast.clone().map(std::sync::Arc::new);
            for &p in &partitions {
                let txp = tx.clone();
                let mut a = args.clone();
                // Override effective args when using a query
                a.topic = Some(topic.clone());
                a.keys_only = keys_only;
                if query_ast.is_some() {
                    a.max_messages = None;
                }
                let q = query_arc.clone();
                let ssl = if args.ssl_ca_pem.is_some()
                    || args.ssl_certificate_pem.is_some()
                    || args.ssl_key_pem.is_some()
                {
                    Some(SslConfig {
                        ca_pem: args.ssl_ca_pem.clone(),
                        cert_pem: args.ssl_certificate_pem.clone(),
                        key_pem: args.ssl_key_pem.clone(),
                    })
                } else {
                    None
                };
                joinset.spawn(async move {
                    spawn_partition_consumer(a, p, offset_spec, txp, q, ssl).await
                });
            }
            drop(tx); // merger will know when producers are done

            // Output sink (table)
            let mut table_out =
                TableOutput::new(args.no_color, columns.clone(), args.max_cell_width);

            // Merge + print
            run_merger(
                rx,
                &mut table_out,
                args.watermark,
                args.flush_interval_ms,
                max_messages,
                order_desc,
            )
            .await?;

            // Await all consumer tasks (and surface errors if any)
            while let Some(res) = joinset.join_next().await {
                res??;
            }

            table_out.finish();
            return Ok(());
        }
    }
}

fn logs_dir() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(|h| std::path::PathBuf::from(h).join(".rkl").join("logs"))
        .unwrap_or_else(|_| std::path::PathBuf::from(".rkl").join("logs"))
}

fn log_cli_error(err: &str) {
    let _ = std::fs::create_dir_all(logs_dir());
    let path = logs_dir().join("cli-error.log");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "".into());
        let _ = writeln!(f, "{} {}", ts, err);
    }
}

fn parse_runargs_from_argv() -> RunArgs {
    let argv: Vec<String> = std::env::args().collect();
    // Accept either: rkl --query "..." or rkl "..."
    // Reuse clap by pretending we're parsing RunArgs as a top-level command
    // If first non-flag arg exists and not starting with '-', treat as query.
    if argv.iter().skip(1).any(|a| a == "--query")
        || argv
            .iter()
            .skip(1)
            .next()
            .map(|a| !a.starts_with('-'))
            .unwrap_or(false)
    {
        let mut args: Vec<String> = vec![argv.get(0).cloned().unwrap_or_else(|| "rkl".into())];
        let mut it = argv.iter().skip(1).cloned();
        let mut consumed_query = false;
        while let Some(a) = it.next() {
            if a == "--query" {
                args.push(a);
                if let Some(q) = it.next() {
                    args.push(q);
                }
                consumed_query = true;
            } else if !a.starts_with('-') && !consumed_query {
                args.push("--query".into());
                args.push(a);
                consumed_query = true;
            } else {
                args.push(a);
            }
        }
        RunArgs::parse_from(args)
    } else {
        RunArgs::parse_from(argv)
    }
}

async fn run_once_cli(args: RunArgs) -> Result<()> {
    // Run the same pipeline as the Run subcommand and log errors
    let res = async {
        // One-time consumer just to fetch metadata / partitions
        let (query_ast, topic, columns, max_messages, order_desc) = if let Some(ref q) = args.query
        {
            let ast = parse_query(q).context("Failed to parse --query")?;
            let columns = ast.select.clone();
            let max_messages = ast.limit.or(args.max_messages);
            let order_desc = ast
                .order
                .as_ref()
                .map(|o| matches!(o.dir, OrderDir::Desc))
                .unwrap_or(false);
            let topic_name = ast.from.clone();
            (Some(ast), topic_name, columns, max_messages, order_desc)
        } else {
            let topic_value = args
                .topic
                .clone()
                .context("topic is required unless --query is provided")?;
            let columns = SelectItem::standard(!args.keys_only);
            (None, topic_value, columns, args.max_messages, false)
        };

        let keys_only = !columns.iter().any(|c| matches!(c, SelectItem::Value));

        let mut probe_cfg = ClientConfig::new();
        probe_cfg
            .set("bootstrap.servers", &args.broker)
            .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "true");
        if args.ssl_ca_pem.is_some()
            || args.ssl_certificate_pem.is_some()
            || args.ssl_key_pem.is_some()
        {
            probe_cfg.set("security.protocol", "ssl");
            if let Some(ref s) = args.ssl_ca_pem {
                probe_cfg.set("ssl.ca.pem", s);
            }
            if let Some(ref s) = args.ssl_certificate_pem {
                probe_cfg.set("ssl.certificate.pem", s);
            }
            if let Some(ref s) = args.ssl_key_pem {
                probe_cfg.set("ssl.key.pem", s);
            }
        }
        let probe_consumer: StreamConsumer = probe_cfg
            .create()
            .context("Failed to create probe consumer")?;

        let metadata = probe_consumer
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .context("Failed to fetch metadata")?;

        let topic_md = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .context("Topic not found")?;

        let partitions: Vec<i32> = if let Some(p) = args.partition {
            vec![p]
        } else {
            topic_md.partitions().iter().map(|p| p.id()).collect()
        };

        let (tx, rx) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);
        let mut joinset = JoinSet::new();
        let offset_spec =
            OffsetSpec::from_str(&args.offset).unwrap_or_else(|_| OffsetSpec::Beginning);
        let query_arc = query_ast.clone().map(std::sync::Arc::new);
        for &p in &partitions {
            let txp = tx.clone();
            let mut a = args.clone();
            a.topic = Some(topic.clone());
            a.keys_only = keys_only;
            if query_ast.is_some() {
                a.max_messages = None;
            }
            let q = query_arc.clone();
            let ssl = if args.ssl_ca_pem.is_some()
                || args.ssl_certificate_pem.is_some()
                || args.ssl_key_pem.is_some()
            {
                Some(SslConfig {
                    ca_pem: args.ssl_ca_pem.clone(),
                    cert_pem: args.ssl_certificate_pem.clone(),
                    key_pem: args.ssl_key_pem.clone(),
                })
            } else {
                None
            };
            joinset.spawn(
                async move { spawn_partition_consumer(a, p, offset_spec, txp, q, ssl).await },
            );
        }
        drop(tx);
        let mut table_out = TableOutput::new(args.no_color, columns.clone(), args.max_cell_width);
        run_merger(
            rx,
            &mut table_out,
            args.watermark,
            args.flush_interval_ms,
            max_messages,
            order_desc,
        )
        .await?;
        while let Some(res) = joinset.join_next().await {
            res??;
        }
        table_out.finish();
        Ok(())
    }
    .await;

    if let Err(ref e) = res {
        log_cli_error(&format!("{}", e));
    }
    res
}
