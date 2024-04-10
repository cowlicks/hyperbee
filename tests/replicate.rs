mod common;
use base64::{engine::general_purpose, Engine as _};
use common::Result;
use hypercore::{prelude::PartialKeypair, HypercoreBuilder, Storage, VerifyingKey};
static _DISCOVERY_KEY: &str = "715P56BzeoA5jKBiV+Lnn7K70JhnJS3wGnp85mCqf24=";
static PUBLIC_KEY: &str = "e4v1DwtWoA0lPYMxzvD3hH0ZTsiOFczptMDjR/mhaKA=";

#[tokio::test]
async fn replicate() -> Result<()> {
    let public: Vec<u8> = general_purpose::STANDARD.decode(PUBLIC_KEY).unwrap();

    let p: [u8; 32] = public.try_into().unwrap();
    let kp = PartialKeypair {
        public: VerifyingKey::from_bytes(&p)?,
        secret: None,
    };
    dbg!(&kp);
    let mut hc = HypercoreBuilder::new(Storage::new_memory().await?)
        .key_pair(kp)
        .build()
        .await?;

    let res = hc.get(0).await?;
    dbg!(&res);
    let res = hc.append(b"foo").await?;
    dbg!(&res);
    let res = hc.get(0).await?;
    dbg!(&res);
    //let hb Hyperbee::from_hypercore(hc)?;

    //Partia
    todo!()
}

/*
#[tokio::test]
async fn example() -> Result<()> {
    use hypercore_protocol::{ProtocolBuilder, Event, Message};
use hypercore_protocol::schema::*;
use async_std::prelude::*;
// Start a tcp server.
let listener = async_std::net::TcpListener::bind("localhost:8000").await.unwrap();
async_std::task::spawn(async move {
    let mut incoming = listener.incoming();
    while let Some(Ok(stream)) = incoming.next().await {
        async_std::task::spawn(async move {
            onconnection(stream, false).await
        });
    }
});

// Connect a client.
let stream = async_std::net::TcpStream::connect("localhost:8000").await.unwrap();
onconnection(stream, true).await;

/// Start Hypercore protocol on a TcpStream.
async fn onconnection (stream: async_std::net::TcpStream, is_initiator: bool) {
    // A peer either is the initiator or a connection or is being connected to.
    let name = if is_initiator { "dialer" } else { "listener" };
    // A key for the channel we want to open. Usually, this is a pre-shared key that both peers
    // know about.
    let key = [3u8; 32];
    // Create the protocol.
    let mut protocol = ProtocolBuilder::new(is_initiator).connect(stream);

    // Iterate over the protocol events. This is required to "drive" the protocol.

    while let Some(Ok(event)) = protocol.next().await {
        eprintln!("{} received event {:?}", name, event);
        match event {
            // The handshake event is emitted after the protocol is fully established.
            Event::Handshake(_remote_key) => {
                protocol.open(key.clone()).await;
            },
            // A Channel event is emitted for each established channel.
            Event::Channel(mut channel) => {
                // A Channel can be sent to other tasks.
                async_std::task::spawn(async move {
                    // A Channel can both send messages and is a stream of incoming messages.
                    channel.send(Message::Want(Want { start: 0, length: 1 })).await;
                    while let Some(message) = channel.next().await {
                        eprintln!("{} received message: {:?}", name, message);
                    }
                });
            },
            _ => {}
        }
    }
}
}
*/
