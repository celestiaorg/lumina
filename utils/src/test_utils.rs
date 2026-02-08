#[cfg(not(target_arch = "wasm32"))]
pub use tokio::test as async_test;
#[cfg(target_arch = "wasm32")]
pub use wasm_bindgen_test::wasm_bindgen_test as async_test;

/// Source the `.env` file and get environment variable.
#[cfg(not(target_arch = "wasm32"))]
pub fn env_var(var_name: &str) -> Option<String> {
    use std::sync::Once;

    static DOTENV: Once = Once::new();
    DOTENV.call_once(|| {
        let _ = dotenvy::dotenv();
    });

    std::env::var(var_name).ok()
}

/// Source the `.env` file and get environment variable.
#[cfg(target_arch = "wasm32")]
pub fn env_var(var_name: &str) -> Option<String> {
    #[derive(rust_embed::Embed)]
    #[folder = "$CARGO_MANIFEST_DIR/../"]
    #[allow_missing = true]
    #[include = ".env*"]
    struct Env;

    let env = Env::get(".env").expect("Couldn't find .env file");

    str::from_utf8(env.data.as_ref())
        .unwrap()
        .lines()
        .find_map(|line| {
            line.split_once('=')
                .take_if(|_| !line.starts_with('#'))
                .take_if(|(name, _)| *name == var_name)
                .map(|(_, var)| var.to_owned())
        })
}
