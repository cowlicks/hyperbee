use random_access_disk::RandomAccessDisk;

use crate::{Hyperbee, HyperbeeError};

#[derive(uniffi::Object)]
// TODO should this be pub?
pub struct DiskBee(Hyperbee<RandomAccessDisk>);

#[derive(uniffi::Record)]
pub struct Gotten {
    seq: u64,
    value: Option<Vec<u8>>,
}

#[derive(uniffi::Record)]
pub struct Putten {
    old_seq: Option<u64>,
    new_seq: u64,
}

#[uniffi::export]
pub async fn hb_from_disk(path_to_storage_dir: &str) -> DiskBee {
    let hb = Hyperbee::from_storage_dir(path_to_storage_dir)
        .await
        .unwrap();
    DiskBee(hb)
}

#[uniffi::export]
impl DiskBee {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Gotten>, HyperbeeError> {
        Ok(self
            .0
            .get(key)
            .await?
            .map(|(seq, value)| Gotten { seq, value }))
    }

    pub async fn put(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<Putten, HyperbeeError> {
        let (old_seq, new_seq) = self.0.put(key, value.as_deref()).await?;
        Ok(Putten { old_seq, new_seq })
    }

    pub async fn del(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.0.del(key).await
    }
}
