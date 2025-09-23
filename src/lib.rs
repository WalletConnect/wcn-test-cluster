use {
    alloy::{
        node_bindings::{Anvil, AnvilInstance},
        signers::local::PrivateKeySigner,
    },
    anyhow::Context,
    base64::Engine as _,
    derive_more::derive::AsRef,
    futures::{StreamExt, stream},
    libp2p_identity::Keypair,
    metrics_exporter_prometheus::PrometheusRecorder,
    std::{
        net::{Ipv4Addr, SocketAddrV4, TcpListener},
        thread,
        time::Duration,
    },
    tap::Pipe as _,
    wcn_cluster::{
        Cluster,
        EncryptionKey,
        PeerId,
        Settings,
        node_operator,
        smart_contract::evm::{self, RpcProvider, Signer},
    },
    wcn_rpc::server::ShutdownSignal,
};

pub struct Config {
    pub bootstrap_node_secret_key: Keypair,
    pub bootstrap_node_port: u16,
    pub client_id: PeerId,
    pub cluster_key: EncryptionKey,
}

#[derive(AsRef, Clone, Copy)]
struct DeploymentConfig {
    #[as_ref]
    encryption_key: EncryptionKey,
}

impl wcn_cluster::Config for DeploymentConfig {
    type SmartContract = evm::SmartContract;
    type KeyspaceShards = ();
    type Node = wcn_cluster::Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: wcn_cluster::Node) -> Self::Node {
        node
    }
}

pub struct ClusterGuard {
    shutdown_signals: Vec<ShutdownSignal>,
    _anvil: AnvilInstance,
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        for signal in &self.shutdown_signals {
            signal.emit();
        }
    }
}

pub async fn run_cluster(cfg: Config) -> anyhow::Result<ClusterGuard> {
    // Spin up a local Anvil instance automatically
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(31337)
        .try_spawn()
        .context("failed to spawn anvil instance")?;

    let settings = Settings {
        max_node_operator_data_bytes: 4096,
        event_propagation_latency: Duration::from_secs(1),
        clock_skew: Duration::from_millis(100),
    };

    // Use Anvil's first key for deployment - convert PrivateKeySigner to our Signer
    let private_key_signer: PrivateKeySigner = anvil.keys().last().unwrap().clone().into();
    let signer =
        Signer::try_from_private_key(&format!("{:#x}", private_key_signer.to_bytes())).unwrap();

    let provider = provider(signer, &anvil).await;

    let cluster_key = cfg.cluster_key;
    let mut node_cfg = Some(cfg);

    let (mut operators, signals): (Vec<_>, Vec<_>) = (1..=5)
        .map(|n| {
            let shutdown_signal = ShutdownSignal::new();

            (
                NodeOperator::new(n, &anvil, node_cfg.take(), shutdown_signal.clone()),
                shutdown_signal,
            )
        })
        .unzip();

    let operators_on_chain = operators.iter().map(NodeOperator::on_chain).collect();

    let cfg = DeploymentConfig {
        encryption_key: cluster_key,
    };

    let cluster = Cluster::deploy(cfg, &provider, settings, operators_on_chain)
        .await
        .context("failed to deploy cluster")?;

    let contract_address = cluster.smart_contract().address();

    operators
        .iter_mut()
        .flat_map(|operator| operator.nodes.as_mut_slice())
        .for_each(|node| node.config.as_mut().unwrap().smart_contract_address = contract_address);

    stream::iter(&mut operators)
        .for_each_concurrent(5, NodeOperator::deploy)
        .await;

    Ok(ClusterGuard {
        shutdown_signals: signals,
        _anvil: anvil,
    })
}

struct NodeOperator {
    signer: Signer,
    name: node_operator::Name,
    database: Database,
    nodes: Vec<Node>,
    clients: Vec<Client>,
    _prometheus_recorder: PrometheusRecorder,
}

struct Client {
    peer_id: PeerId,
    authorized_namespaces: Vec<u8>,
}

impl Client {
    fn on_chain(&self) -> wcn_cluster::Client {
        wcn_cluster::Client {
            peer_id: self.peer_id,
            authorized_namespaces: self.authorized_namespaces.clone().into(),
        }
    }
}

struct Database {
    config: Option<wcn_db::Config>,
    thread_handle: Option<thread::JoinHandle<()>>,
    _shutdown_signal: ShutdownSignal,
}

impl Database {
    fn deploy(&mut self) {
        let fut = wcn_db::run(self.config.take().unwrap()).unwrap();

        self.thread_handle = Some(thread::spawn(move || {
            let _guard = tracing::info_span!("database").entered();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(fut);
        }));
    }
}

struct Node {
    operator_id: node_operator::Id,
    config: Option<wcn_node::Config>,
    thread_handle: Option<thread::JoinHandle<()>>,
    _shutdown_signal: ShutdownSignal,
}

impl Node {
    async fn deploy(&mut self) {
        let operator_id = self.operator_id;
        let fut = wcn_node::run(self.config.take().unwrap());

        let (tx, rx) = std::sync::mpsc::channel::<wcn_node::Result<()>>();

        self.thread_handle = Some(thread::spawn(move || {
            let _guard = tracing::info_span!("node", %operator_id).entered();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    match fut.await {
                        Ok(fut) => {
                            let _ = tx.send(Ok(()));
                            fut.await
                        }
                        Err(err) => tx.send(Err(err)).pipe(drop),
                    }
                });
        }));

        tokio::task::spawn_blocking(move || rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn on_chain(&self) -> wcn_cluster::Node {
        wcn_cluster::Node {
            peer_id: self.config.as_ref().unwrap().keypair.public().to_peer_id(),
            ipv4_addr: Ipv4Addr::LOCALHOST,
            primary_port: self
                .config
                .as_ref()
                .unwrap()
                .primary_rpc_server_socket
                .port()
                .unwrap(),
            secondary_port: self
                .config
                .as_ref()
                .unwrap()
                .secondary_rpc_server_socket
                .port()
                .unwrap(),
        }
    }
}

impl NodeOperator {
    fn new(
        id: u8,
        anvil: &AnvilInstance,
        mut cfg: Option<Config>,
        shutdown_signal: ShutdownSignal,
    ) -> NodeOperator {
        // dummy value, we don't know the address at this point yet
        let smart_contract_address = "0xF85FA2ce74D0b65756E14377f0359BB13E229ECE"
            .parse()
            .unwrap();

        let smart_contract_signer = anvil.keys()[id as usize]
            .to_bytes()
            .pipe(|bytes| Signer::try_from_private_key(&const_hex::encode(bytes)).unwrap());

        let operator_id = *smart_contract_signer.address();

        let rpc_provider_url = anvil.endpoint_url().to_string().replace("http://", "ws://");

        let db_keypair = Keypair::generate_ed25519();
        let db_peer_id = db_keypair.public().to_peer_id();

        let prometheus_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();

        let db_primary_socket = wcn_rpc::server::Socket::new_high_priority(0).unwrap();
        let db_primary_port = db_primary_socket.port().unwrap();
        let db_secondary_socket = wcn_rpc::server::Socket::new_low_priority(0).unwrap();
        let db_secondary_port = db_secondary_socket.port().unwrap();

        let db_config = wcn_db::Config {
            keypair: db_keypair,
            primary_rpc_server_socket: db_primary_socket,
            secondary_rpc_server_socket: db_secondary_socket,
            metrics_server_socket: new_tcp_socket(),
            connection_timeout: Duration::from_secs(1),
            max_connections: 1000,
            max_connections_per_ip: 1000,
            max_connection_rate_per_ip: 1000,
            max_concurrent_rpcs: 5000,
            max_idle_connection_timeout: Duration::from_secs(1),
            rocksdb_dir: format!("/tmp/wcn_db/{db_peer_id}").parse().unwrap(),
            rocksdb: Default::default(),
            shutdown_signal: shutdown_signal.clone(),
            prometheus_handle: prometheus_recorder.handle(),
        };

        let database = Database {
            config: Some(db_config),
            thread_handle: None,
            _shutdown_signal: shutdown_signal.clone(),
        };

        let client_id = cfg
            .as_ref()
            .map(|cfg| cfg.client_id)
            .unwrap_or_else(|| Keypair::generate_ed25519().public().to_peer_id());

        let nodes = (0..=1)
            .map(|n| {
                let (keypair, primary_rpc_server_port) = cfg
                    .take()
                    .map(|cfg| (cfg.bootstrap_node_secret_key, cfg.bootstrap_node_port))
                    .unwrap_or_else(|| (Keypair::generate_ed25519(), 0));
                let primary_rpc_server_socket =
                    wcn_rpc::server::Socket::new_high_priority(primary_rpc_server_port).unwrap();
                let secondary_rpc_server_socket =
                    wcn_rpc::server::Socket::new_low_priority(0).unwrap();

                let config = wcn_node::Config {
                    keypair,
                    primary_rpc_server_socket,
                    secondary_rpc_server_socket,
                    metrics_server_socket: new_tcp_socket(),
                    max_idle_connection_timeout: Duration::from_secs(1),
                    database_rpc_server_address: Ipv4Addr::LOCALHOST,
                    database_peer_id: db_peer_id,
                    database_primary_rpc_server_port: db_primary_port,
                    database_secondary_rpc_server_port: db_secondary_port,
                    smart_contract_address,
                    smart_contract_signer: (n == 0).then_some(smart_contract_signer.clone()),
                    smart_contract_encryption_key: wcn_cluster::testing::encryption_key(),
                    rpc_provider_url: rpc_provider_url.clone().parse().unwrap(),
                    shutdown_signal: shutdown_signal.clone(),
                    prometheus_handle: prometheus_recorder.handle(),
                };

                Node {
                    operator_id,
                    config: Some(config),
                    _shutdown_signal: shutdown_signal.clone(),
                    thread_handle: None,
                }
            })
            .collect();

        Self {
            signer: smart_contract_signer,
            name: node_operator::Name::new(format!("operator{id}")).unwrap(),
            database,
            nodes,
            clients: vec![Client {
                peer_id: client_id,
                authorized_namespaces: vec![0, 1],
            }],
            _prometheus_recorder: prometheus_recorder,
        }
    }

    async fn deploy(&mut self) {
        self.database.deploy();

        self.nodes
            .iter_mut()
            .pipe(stream::iter)
            .for_each_concurrent(10, Node::deploy)
            .await;
    }

    fn on_chain(&self) -> wcn_cluster::NodeOperator {
        wcn_cluster::NodeOperator::new(
            *self.signer.address(),
            self.name.clone(),
            self.nodes.iter().map(Node::on_chain).collect(),
            self.clients.iter().map(Client::on_chain).collect(),
        )
        .unwrap()
    }
}

async fn provider(signer: Signer, anvil: &AnvilInstance) -> RpcProvider {
    let ws_url = anvil.endpoint_url().to_string().replace("http://", "ws://");
    RpcProvider::new(ws_url.parse().unwrap(), signer)
        .await
        .unwrap()
}

fn new_tcp_socket() -> TcpListener {
    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).unwrap()
}

pub fn parse_secret_key(key: &str) -> anyhow::Result<Keypair> {
    let key = base64::engine::general_purpose::STANDARD.decode(key)?;
    Keypair::ed25519_from_bytes(key).map_err(Into::into)
}

pub fn parse_cluster_key(key: &str) -> anyhow::Result<EncryptionKey> {
    let key = const_hex::decode(key)?[..].try_into()?;
    Ok(EncryptionKey(key))
}
