use std::time::Duration;

use replicator::utils::{create_connected_streams, writer_and_reader_cores};

use crate::Hyperbee;

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
            if let Some((_, Some(x))) = $core.get($key).await? {
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
    writer_bee.add_stream(a_to_b, false).await?;
    reader_bee.add_stream(b_to_a, true).await?;

    writer_bee.put(b"hello", Some(b"world")).await?;
    assert_gets!(reader_bee, b"hello", b"world");
    Ok(())
}
