use {anyhow::Context, serde::Deserialize};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

#[derive(Debug, Deserialize)]
struct EnvConfig {
    bootstrap_node_secret_key: String,
    bootstrap_node_port: u16,
    client_id: String,
    cluster_encryption_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logger = wcn_logging::Logger::init(wcn_logging::LogFormat::Text, None, None);
    let env: EnvConfig = envy::from_env()?;

    let cfg = wcn_test_cluster::Config {
        bootstrap_node_secret_key: wcn_test_cluster::parse_secret_key(
            &env.bootstrap_node_secret_key,
        )
        .context("failed to parse `bootstrap_node_secret_key`")?,
        bootstrap_node_port: env.bootstrap_node_port,
        client_id: env
            .client_id
            .parse()
            .context("failed to parse `client_id`")?,
        cluster_encryption_key: wcn_test_cluster::parse_encryption_key(&env.cluster_encryption_key)
            .context("failed to parse `cluster_encryption_key`")?,
    };

    let _guard = wcn_test_cluster::run_cluster(cfg).await?;

    tokio::signal::ctrl_c().await?;

    tracing::info!("shutting down");

    Ok(())
}
