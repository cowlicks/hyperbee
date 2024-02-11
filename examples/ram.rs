use hyperbee_rs::Hyperbee;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = Hyperbee::from_ram().await?;
    // Insert "world" with key "hello"
    hb.put(b"hello", Some(b"world")).await?;

    // Get the value for key "hello"
    let Some((_seq, Some(val)))  = hb.get(b"hello").await? else {
        panic!("could not get value");
    };
    assert_eq!(val, b"world");

    // Trying to get a non-exsitant key returns None
    let res = hb.get(b"no key here").await?;
    assert_eq!(res, None);

    // deleting a key returns `true` if it was present
    let res = hb.del(b"hello").await?;
    assert_eq!(res, true);
    Ok(())
}