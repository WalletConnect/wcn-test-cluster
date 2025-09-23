use {
    anyhow::Context,
    serde::Deserialize,
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    },
    wcn_client::PeerAddr,
};

#[derive(Debug, Deserialize)]
struct EnvConfig {
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

    let env: EnvConfig = envy::from_env()?;

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
    })
    .build()
    .await
    .context("failed to build client")?;

    tracing::info!("client connection successful");

    let namespace = env.client_namespace.parse().unwrap();

    client
        .set(namespace, b"foo", b"bar", Duration::from_secs(60))
        .await
        .unwrap();

    let rec = client.get(namespace, b"foo").await.unwrap().unwrap();
    assert_eq!(rec.value, b"bar");

    tracing::info!("storage operations successful");

    Ok(())
}
