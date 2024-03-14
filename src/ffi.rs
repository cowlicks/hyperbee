use crate::{Hyperbee, HyperbeeError};

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

#[uniffi::export]
impl Hyperbee {
    // TODO https://github.com/mozilla/uniffi-rs/issues/2031#issuecomment-1998464424
    // use git version to rename these
    async fn ffi_get(&self, key: &[u8]) -> Result<Option<Gotten>, HyperbeeError> {
        Ok(self
            .get(key)
            .await?
            .map(|(seq, value)| Gotten::new(seq, value)))
    }

    async fn ffi_put(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<Putten, HyperbeeError> {
        let (old_seq, new_seq) = self.put(key, value.as_deref()).await?;
        Ok(Putten::new(old_seq, new_seq))
    }

    pub async fn ffi_del(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.del(key).await
    }
}

#[uniffi::export]
async fn hyperbee_from_ram() -> Result<Hyperbee, HyperbeeError> {
    Hyperbee::from_ram().await
}
