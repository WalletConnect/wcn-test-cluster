use {
    anyhow::Context,
    serde::Deserialize,
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        process::{Command, Stdio},
        time::Duration,
    },
    wcn_client::PeerAddr,
};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

#[derive(Debug, Deserialize)]
struct ServerConfig {
    bootstrap_node_secret_key: String,
    bootstrap_node_port: u16,
    client_id: String,
    cluster_key: String,
    detach: bool,
    internal_detached: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ClientConfig {
    client_secret_key: String,
    client_namespace: String,
    bootstrap_node_id: String,
    bootstrap_node_address: Ipv4Addr,
    bootstrap_node_port: u16,
    cluster_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logger = wcn_logging::Logger::init(wcn_logging::LogFormat::Text, None, None);
    let server_env = serde_env::from_env::<ServerConfig>()?;

    if server_env.detach && !server_env.internal_detached.unwrap_or(false) {
        let args = std::env::args().collect::<Vec<_>>();
        let (_, args) = args.split_first().unwrap();
        let exec = std::env::current_exe()?;

        let mut command = Command::new(exec);

        let mut child = command
            .env("INTERNAL_DETACHED", "true")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .args(args)
            .spawn()
            .context("failed to spawn child process")?;

        let client_env = serde_env::from_env::<ClientConfig>()?;

        test_connection(&client_env)
            .await
            .context("failed to test client connection")
            .inspect_err(|_| {
                let _ = child.kill();
            })?;

        Ok(())
    } else {
        run(&server_env).await
    }
}

async fn run(env: &ServerConfig) -> anyhow::Result<()> {
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
        cluster_key: wcn_test_cluster::parse_cluster_key(&env.cluster_key)
            .context("failed to parse `cluster_key`")?,
    };

    let _guard = wcn_test_cluster::run_cluster(cfg).await?;

    tokio::signal::ctrl_c().await?;

    tracing::info!("shutting down");

    Ok(())
}

async fn test_connection(env: &ClientConfig) -> anyhow::Result<()> {
    let bootstrap_node_id = env
        .bootstrap_node_id
        .parse()
        .context("failed to parse `bootstrap_node_id`")?;

    let bootstrap_node = PeerAddr {
        id: bootstrap_node_id,
        addr: SocketAddrV4::new(env.bootstrap_node_address, env.bootstrap_node_port),
    };

    tracing::info!(addr = ?bootstrap_node, "bootstrap node");

    let keypair = wcn_test_cluster::parse_secret_key(&env.client_secret_key)
        .context("failed to parse `client_secret_key`")?;

    tracing::info!(peer_id = ?keypair.public().to_peer_id(), "client peer ID");

    let cluster_key = wcn_test_cluster::parse_cluster_key(&env.cluster_key)
        .context("failed to parse `cluster_key`")?;

    let client = wcn_client::Client::builder(wcn_client::Config {
        keypair,
        cluster_key,
        connection_timeout: Duration::from_secs(3),
        operation_timeout: Duration::from_secs(5),
        reconnect_interval: Duration::from_millis(100),
        max_concurrent_rpcs: 5000,
        max_idle_connection_timeout: Duration::from_secs(1),
        max_retries: 2,
        nodes: vec![bootstrap_node],
        trusted_operators: Default::default(),
    })
    .build()
    .await
    .context("failed to build client")?;

    tracing::info!("client connection successful");

    let namespace = env
        .client_namespace
        .parse()
        .context("failed to parse client namespace")?;

    let check_connection = || async {
        client
            .set(namespace, b"foo", b"bar", Duration::from_secs(60))
            .await?;

        if let Some(rec) = client.get(namespace, b"foo").await?
            && rec.value == b"bar"
        {
            tracing::info!("storage operations successful");

            Ok(())
        } else {
            Err(anyhow::format_err!("invalid storage operation result"))
        }
    };

    let mut attempt = 0;

    while let Err(err) = check_connection().await {
        tracing::warn!(?err, "client connection failed");

        if attempt < 3 {
            attempt += 1;

            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            return Err(err);
        }
    }

    Ok(())
}
