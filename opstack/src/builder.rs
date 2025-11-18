use eyre::Result;
use helios_core::execution::providers::{
    block::block_cache::BlockCache, 
    freezer::block_freezer::BlockFreezer,
    historical::eip2935::Eip2935Provider,
    rpc::RpcExecutionProvider, 
    verifiable_api::VerifiableApiExecutionProvider,
};
use reqwest::{IntoUrl, Url};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::{
    config::{Config, Network, NetworkConfig},
    consensus::ConsensusClient,
    spec::OpStack,
    OpStackClient,
};

#[derive(Default)]
pub struct OpStackClientBuilder {
    config: Option<Config>,
    network: Option<Network>,
    consensus_rpc: Option<Url>,
    execution_rpc: Option<Url>,
    verifiable_api: Option<Url>,
    rpc_socket: Option<SocketAddr>,
    verify_unsafe_signer: Option<bool>,
}

impl OpStackClientBuilder {
    pub fn new() -> Self {
        OpStackClientBuilder::default()
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn consensus_rpc<T: IntoUrl>(mut self, consensus_rpc: T) -> Self {
        self.consensus_rpc = Some(consensus_rpc.into_url().unwrap());
        self
    }

    pub fn execution_rpc<T: IntoUrl>(mut self, execution_rpc: T) -> Self {
        self.execution_rpc = Some(execution_rpc.into_url().unwrap());
        self
    }

    pub fn verifiable_api<T: IntoUrl>(mut self, verifiable_api: T) -> Self {
        self.verifiable_api = Some(verifiable_api.into_url().unwrap());
        self
    }

    pub fn rpc_socket(mut self, socket: SocketAddr) -> Self {
        self.rpc_socket = Some(socket);
        self
    }

    pub fn network(mut self, network: Network) -> Self {
        self.network = Some(network);
        self
    }

    pub fn verify_unsafe_signer(mut self, value: bool) -> Self {
        self.verify_unsafe_signer = Some(value);
        self
    }

    pub fn build(self) -> Result<OpStackClient> {
        let config = if let Some(config) = self.config {
            config
        } else {
            let Some(network) = self.network else {
                eyre::bail!("network required");
            };

            let Some(consensus_rpc) = self.consensus_rpc else {
                eyre::bail!("consensus rpc required");
            };

            Config {
                consensus_rpc,
                execution_rpc: self.execution_rpc,
                verifiable_api: self.verifiable_api,
                rpc_socket: self.rpc_socket,
                chain: NetworkConfig::from(network).chain,
                load_external_fallback: None,
                checkpoint: None,
                verify_unsafe_signer: self.verify_unsafe_signer.unwrap_or_default(),
                data_dir: None,
                archive_from: None,
            }
        };

        let consensus = ConsensusClient::new(&config);

        let block_provider = BlockCache::<OpStack>::new();

        if let Some(archive_from) = config.archive_from {
            let block_freezer = Arc::new(BlockFreezer::new(
                archive_from,
                config.data_dir.clone(),
                Eip2935Provider::new(),
            ));

            if let Some(verifiable_api) = &config.verifiable_api {
                let execution = Arc::new(VerifiableApiExecutionProvider::with_freezer_provider(
                    verifiable_api,
                    block_provider,
                    Eip2935Provider::new(),
                    Arc::clone(&block_freezer),
                ));

                if let Err(e) = block_freezer.start_sync(Arc::clone(&execution)) {
                    eprintln!("Failed to start block freezer sync: {}", e);
                }

                Ok(OpStackClient::new(
                    consensus,
                    execution,
                    config.chain.forks,
                    #[cfg(not(target_arch = "wasm32"))]
                    config.rpc_socket,
                ))
            } else {
                let rpc_url = config.execution_rpc.as_ref().unwrap().clone();
                let execution = Arc::new(RpcExecutionProvider::with_freezer_provider(
                    rpc_url,
                    block_provider,
                    Eip2935Provider::new(),
                    Arc::clone(&block_freezer),
                ));

                if let Err(e) = block_freezer.start_sync(Arc::clone(&execution)) {
                    eprintln!("Failed to start block freezer sync: {}", e);
                }

                Ok(OpStackClient::new(
                    consensus,
                    execution,
                    config.chain.forks,
                    #[cfg(not(target_arch = "wasm32"))]
                    config.rpc_socket,
                ))
            }
        } else {
            if let Some(verifiable_api) = &config.verifiable_api {
                let execution =
                    Arc::new(VerifiableApiExecutionProvider::with_historical_provider(
                        verifiable_api,
                        block_provider,
                        Eip2935Provider::new(),
                    ));

                Ok(OpStackClient::new(
                    consensus,
                    execution,
                    config.chain.forks,
                    #[cfg(not(target_arch = "wasm32"))]
                    config.rpc_socket,
                ))
            } else {
                let rpc_url = config.execution_rpc.as_ref().unwrap().clone();
                let execution = Arc::new(RpcExecutionProvider::with_historical_provider(
                    rpc_url,
                    block_provider,
                    Eip2935Provider::new(),
                ));

                Ok(OpStackClient::new(
                    consensus,
                    execution,
                    config.chain.forks,
                    #[cfg(not(target_arch = "wasm32"))]
                    config.rpc_socket,
                ))
            }
        }
    }
}
