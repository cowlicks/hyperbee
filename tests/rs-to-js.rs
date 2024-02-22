mod js;

#[cfg(test)]
mod rs_to_js {
    use super::js::run_js;
    use hyperbee::Hyperbee;

    #[tokio::test]
    async fn hello_world() -> Result<(), Box<dyn std::error::Error>> {
        let storage_dir = tempfile::tempdir()?;
        let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
        let key = b"hello";
        let value = b"world";
        hb.put(key, Some(b"world")).await?;
        let res = hb.get(b"hello").await?;
        assert_eq!(res, Some((1u64, Some(value.to_vec()))));
        let output = run_js(
            &storage_dir,
            "
const r = await hb.get('hello');
write(r.value.toString());",
        )?;
        assert_eq!(output.stdout, b"world");
        Ok(())
    }

    #[tokio::test]
    async fn zero_to_one_hundred() -> Result<(), Box<dyn std::error::Error>> {
        let storage_dir = tempfile::tempdir()?;
        let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|x| x.clone().to_string().as_bytes().to_vec())
            .collect();

        for k in keys.iter() {
            let val: Option<&[u8]> = Some(k);
            hb.put(k, val).await?;
        }
        let output = run_js(
            &storage_dir,
            "
    const out = [];
    for (let i = 0; i < 100; i++) {
        out.push(
        (await hb.get(String(i))).value.toString()
        );
    }
    write(JSON.stringify(out));
    ",
        )?;
        let stdout = String::from_utf8(output.stdout)?;
        let res: Vec<String> = serde_json::from_str(&stdout)?;
        let res: Vec<Vec<u8>> = res.into_iter().map(|x| x.into()).collect();
        assert_eq!(res, keys);
        Ok(())
    }
}
