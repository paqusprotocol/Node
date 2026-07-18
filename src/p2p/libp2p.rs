//! libp2p swarm adapter.

use crate::runtime::network::{NetworkEnvelope, NetworkMessage};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, gossipsub, identify, identity, ping,
    request_response, swarm::NetworkBehaviour,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const PAQUS_BLOCK_TOPIC: &str = "paqus/devnet/blocks/1";
pub const PAQUS_TX_TOPIC: &str = "paqus/devnet/tx/1";
pub const PAQUS_REQUEST_PROTOCOL: &str = "/paqus/devnet/sync/1";

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Libp2pCommand {
    Dial(Multiaddr),
    PublishBlock(NetworkMessage),
    PublishTransaction(NetworkMessage),
    Request(PeerId, NetworkMessage),
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Libp2pEvent {
    Message {
        source: Option<PeerId>,
        message: NetworkMessage,
    },
    Request {
        peer: PeerId,
        message: NetworkMessage,
    },
    Response {
        peer: PeerId,
        message: NetworkMessage,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    bytes: Vec<u8>,
}

impl WireMessage {
    #[allow(dead_code)]
    pub fn encode(message: &NetworkMessage) -> Result<Self, String> {
        let envelope = message.clone().to_envelope();
        let bytes = envelope
            .to_bytes()
            .map_err(|error| format!("failed to encode network envelope: {error}"))?;
        Ok(Self { bytes })
    }

    #[allow(dead_code)]
    pub fn decode(self) -> Result<NetworkMessage, String> {
        NetworkEnvelope::from_bytes(&self.bytes)
            .map(|envelope| envelope.message)
            .map_err(|error| format!("failed to decode network envelope: {error}"))
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "PaqusBehaviourEvent")]
pub struct PaqusBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::json::Behaviour<WireMessage, WireMessage>,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
}

#[allow(dead_code)]
#[allow(clippy::large_enum_variant)] // Variant sizes are fixed by libp2p event types.
#[derive(Debug)]
pub enum PaqusBehaviourEvent {
    Gossipsub(gossipsub::Event),
    RequestResponse(request_response::Event<WireMessage, WireMessage>),
    Identify(identify::Event),
    Ping(ping::Event),
}

impl From<gossipsub::Event> for PaqusBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(event)
    }
}

impl From<request_response::Event<WireMessage, WireMessage>> for PaqusBehaviourEvent {
    fn from(event: request_response::Event<WireMessage, WireMessage>) -> Self {
        Self::RequestResponse(event)
    }
}

impl From<identify::Event> for PaqusBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(event)
    }
}

impl From<ping::Event> for PaqusBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

pub fn build_swarm() -> Result<Swarm<PaqusBehaviour>, String> {
    Ok(SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            Default::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .map_err(|error| format!("failed to build libp2p tcp transport: {error}"))?
        .with_behaviour(build_behaviour)
        .map_err(|error| format!("failed to build libp2p behaviour: {error}"))?
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(60)))
        .build())
}

fn build_behaviour(keypair: &identity::Keypair) -> PaqusBehaviour {
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .build()
        .expect("static gossipsub config should be valid");
    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .expect("static gossipsub behaviour should be valid");
    gossipsub
        .subscribe(&block_topic())
        .expect("static block topic should be valid");
    gossipsub
        .subscribe(&transaction_topic())
        .expect("static transaction topic should be valid");

    let request_response = request_response::json::Behaviour::new(
        [(
            StreamProtocol::new(PAQUS_REQUEST_PROTOCOL),
            request_response::ProtocolSupport::Full,
        )],
        request_response::Config::default().with_request_timeout(Duration::from_secs(10)),
    );

    PaqusBehaviour {
        gossipsub,
        request_response,
        identify: identify::Behaviour::new(identify::Config::new(
            PAQUS_REQUEST_PROTOCOL.to_string(),
            keypair.public(),
        )),
        ping: ping::Behaviour::new(ping::Config::new()),
    }
}

pub fn block_topic() -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(PAQUS_BLOCK_TOPIC)
}

pub fn transaction_topic() -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(PAQUS_TX_TOPIC)
}
