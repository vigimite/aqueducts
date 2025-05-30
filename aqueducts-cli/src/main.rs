use anyhow::anyhow;
use clap::{Parser, Subcommand};
use std::{collections::HashMap, error::Error, path::PathBuf};
use tracing::info;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use uuid::Uuid;

mod local_exec;
mod remote_exec;
mod websocket_client;

/// Aqueducts CLI for executing data pipelines locally or remotely
#[derive(Debug, Parser)]
#[command(name = "aqueducts", version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run an Aqueduct pipeline locally or remotely
    Run {
        /// Path to Aqueduct configuration file
        #[arg(short, long)]
        file: PathBuf,

        /// k=v list of parameters to pass to the configuration file
        /// e.g. aqueduct run -f file.yml -p key1=value1 -p key2=value2
        #[arg(short, long, value_parser = parse_key_val::<String, String>)]
        params: Option<Vec<(String, String)>>,

        /// Execute the pipeline on a remote executor instead of locally
        /// example: 192.168.1.102:3031
        #[arg(long)]
        executor: Option<String>,

        /// API key for the remote executor
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Cancel a running pipeline on a remote executor
    Cancel {
        /// Execution ID to cancel
        #[arg(short, long)]
        execution_id: String,

        /// Remote executor URL
        /// example: 192.168.1.102:3031
        #[arg(long)]
        executor: String,

        /// API key for the remote executor
        #[arg(long)]
        api_key: String,
    },
}

fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(true)
                .with_level(false)
                .with_target(false)
                .without_time()
                .with_filter(filter::filter_fn(|meta| !meta.is_span())),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Run {
            file,
            params,
            executor: Some(executor_url),
            api_key,
        } => {
            let api_key =
                api_key.ok_or_else(|| anyhow!("API key is required for remote execution"))?;

            info!("Executing pipeline on remote executor: {}", executor_url);
            let params = HashMap::from_iter(params.unwrap_or_default());
            remote_exec::run_remote(file, params, executor_url, api_key).await?;
        }
        Commands::Run {
            file,
            params,
            executor: _,
            api_key: _,
        } => {
            let params = HashMap::from_iter(params.unwrap_or_default());
            local_exec::run_local(file, params).await?;
        }
        Commands::Cancel {
            execution_id,
            executor,
            api_key,
        } => {
            let execution_id = Uuid::parse_str(&execution_id)
                .map_err(|e| anyhow!("Invalid execution ID: {}. Must be a valid UUID.", e))?;

            info!(
                "Cancelling execution {} on executor: {}",
                execution_id, executor
            );
            remote_exec::cancel_remote_execution(executor, api_key, execution_id).await?;
        }
    }

    Ok(())
}
