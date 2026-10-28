use std::time::Duration;

use hypercore::{Hypercore, HypercoreBuilder, PartialKeypair, Storage};
use hypercore_handshake::{
    state_machine::{hc_specific::generate_keypair, SecStream},
    Cipher, CipherTrait,
};
use tokio_util::compat::TokioAsyncReadCompatExt;
use uint24le_framing::Uint24LELengthPrefixedFraming;

use crate::Hyperbee;

/// Create a pair of connected in-memory encrypted streams.
fn create_connected_streams() -> (impl CipherTrait + 'static, impl CipherTrait + 'static) {
    let (a_b, b_a) = tokio::io::duplex(64 * 1024);
    let a_b = Uint24LELengthPrefixedFraming::new(a_b.compat());
    let b_a = Uint24LELengthPrefixedFraming::new(b_a.compat());
    let keypair = generate_keypair().unwrap();
    let initiator = Cipher::new(
        Some(Box::new(a_b)),
        SecStream::new_initiator_xx(&[]).unwrap().into(),
    );
    let responder = Cipher::new(
        Some(Box::new(b_a)),
        SecStream::new_responder_xx(&keypair, &[]).unwrap().into(),
    );
    (initiator, responder)
}

/// Create a writer [`Hypercore`] and a blank reader sharing the same public key.
async fn writer_and_reader_cores() -> Result<(Hypercore, Hypercore), Box<dyn std::error::Error>> {
    let writer = HypercoreBuilder::new(Storage::new_memory().await?)
        .build()
        .await?;
    let public_key = writer.key_pair().public;
    let reader = HypercoreBuilder::new(Storage::new_memory().await?)
        .key_pair(PartialKeypair {
            public: public_key,
            secret: None,
        })
        .build()
        .await?;
    Ok((writer, reader))
}

const MAX_LOOPS: usize = 25;

macro_rules! assert_gets {
    ($core:expr, $key:expr, $val:expr) => {
        let mut n_loops = 0;
        loop {
            if n_loops > MAX_LOOPS {
                panic!(
                    "Hb::get for key {} took too long",
                    String::from_utf8_lossy($key)
                );
            }
            if let Ok(Some((_, Some(x)))) = $core.get($key).await {
                assert_eq!(x, $val);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            n_loops += 1;
        }
    };
}

#[tokio::test]
async fn hello_world() -> Result<(), Box<dyn std::error::Error>> {
    let (writer_core, reader_core) = writer_and_reader_cores().await?;
    let (a_to_b, b_to_a) = create_connected_streams();
    let writer_bee = Hyperbee::from_hypercore(writer_core)?;
    let reader_bee = Hyperbee::from_hypercore(reader_core)?;
    writer_bee.add_stream(a_to_b).await?;
    reader_bee.add_stream(b_to_a).await?;

    writer_bee.put(b"hello", Some(b"world")).await?;
    assert_gets!(reader_bee, b"hello", b"world");
    Ok(())
}
