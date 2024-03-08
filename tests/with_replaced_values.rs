mod common;
use common::join_paths;

use crate::common::{get_data_dir, js::require_js_data};
static HYPERBEE_STORAGE_DIR: &str = "with_replaced_values";

#[tokio::test]
async fn with_replaced_values() -> Result<(), Box<dyn std::error::Error>> {
    require_js_data()?;
    let start = 0;
    let stop = 25;
    let storage_dir = join_paths!(get_data_dir()?, &HYPERBEE_STORAGE_DIR);
    let hb = hyperbee::Hyperbee::from_storage_dir(storage_dir).await?;
    for i in start..stop {
        let key = i.to_string();
        let expected = (i * 2).to_string();

        let (seq, res) = hb.get(key.as_bytes()).await?.unwrap();
        let res = res.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        println!("i {i} seq {seq} res {res}");
        assert_eq!(res, expected);
    }
    Ok(())
}
