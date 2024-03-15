use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{Hyperbee as RustHyperbee, HyperbeeError, Shared};

#[derive(Debug, uniffi::Record)]
pub struct Gotten {
    pub seq: u64,
    pub value: Option<Vec<u8>>,
}

impl Gotten {
    fn new(seq: u64, value: Option<Vec<u8>>) -> Self {
        Self { seq, value }
    }
}

#[derive(Debug, uniffi::Record)]
pub struct Putten {
    pub old_seq: Option<u64>,
    pub new_seq: u64,
}

impl Putten {
    fn new(old_seq: Option<u64>, new_seq: u64) -> Self {
        Self { old_seq, new_seq }
    }
}

/// NB: this shadows [`crate::Hyperbee`], but it is not exposed in the public Rust API. We use the
/// same name because we want the FFI API to use the Hyperbee name too.
#[derive(Debug, uniffi::Object)]
struct Hyperbee {
    rust_hyperbee: Shared<RustHyperbee>,
}

#[uniffi::export]
impl Hyperbee {
    async fn get(&self, key: &[u8]) -> Result<Option<Gotten>, HyperbeeError> {
        Ok(self
            .rust_hyperbee
            .read()
            .await
            .get(key)
            .await?
            .map(|(seq, value)| Gotten::new(seq, value)))
    }

    async fn put(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<Putten, HyperbeeError> {
        let (old_seq, new_seq) = self
            .rust_hyperbee
            .read()
            .await
            .put(key, value.as_deref())
            .await?;
        Ok(Putten::new(old_seq, new_seq))
    }

    async fn delete(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.rust_hyperbee.read().await.del(key).await
    }
}

#[uniffi::export]
async fn hyperbee_from_ram() -> Result<Hyperbee, HyperbeeError> {
    let rust_hyperbee = RustHyperbee::from_ram().await?;
    Ok(Hyperbee {
        rust_hyperbee: Arc::new(RwLock::new(rust_hyperbee)),
    })
}
