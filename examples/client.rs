use {
    anyhow::Context,
    serde::Deserialize,
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    },
    wcn_client::{ClientBuilder as _, PeerAddr},
};

#[derive(Debug, Deserialize)]
struct EnvConfig {
    client_secret_key: String,
    client_namespace: String,
    bootstrap_node_id: String,
    bootstrap_node_port: u16,
    cluster_encryption_key: String,
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
        addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, env.bootstrap_node_port),
    };

    let keypair = wcn_test_cluster::parse_secret_key(&env.client_secret_key)
        .context("failed to parse `client_secret_key`")?;
    let cluster_encryption_key =
        wcn_test_cluster::parse_encryption_key(&env.cluster_encryption_key)
            .context("failed to parse `cluster_encryption_key`")?;

    let client = wcn_client::Client::new(wcn_client::Config {
        keypair,
        cluster_encryption_key,
        connection_timeout: Duration::from_secs(1),
        operation_timeout: Duration::from_secs(2),
        reconnect_interval: Duration::from_millis(100),
        max_concurrent_rpcs: 5000,
        nodes: vec![bootstrap_node],
        metrics_tag: "mainnet",
    })
    .await
    .context("failed to build client")?
    .build();

    tracing::info!("client connection successful");

    // Give some time for the client to open connections to the nodes.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let namespace = env.client_namespace.parse().unwrap();

    client
        .set(namespace, b"foo", b"bar", Duration::from_secs(60))
        .await
        .unwrap();

    let res = client.get(namespace, b"foo").await.unwrap();
    assert_eq!(res, Some(b"bar".into()));

    tracing::info!("storage operations successful");

    Ok(())
}
