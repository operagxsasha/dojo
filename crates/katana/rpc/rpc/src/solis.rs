use base64::decode;
use std::sync::Arc;

use jsonrpsee::core::{async_trait, Error};
use katana_core::hooker::HookerAddresses;
use katana_core::sequencer::KatanaSequencer;
use katana_executor::ExecutorFactory;
use katana_rpc_api::solis::SolisApiServer;

pub struct SolisApi<EF: ExecutorFactory> {
    sequencer: Arc<KatanaSequencer<EF>>,
    pub rpc_user: String,
    pub rpc_password: String,
}

impl<EF: ExecutorFactory> SolisApi<EF> {
    pub fn new(sequencer: Arc<KatanaSequencer<EF>>) -> Self {
        Self { sequencer, rpc_user: "".to_string(), rpc_password: "".to_string() }
    }

    fn verify_basic_auth(&self, encoded_credentials: &str) -> bool {
        if let Ok(credentials) = decode(encoded_credentials) {
            if let Ok(credentials_str) = String::from_utf8(credentials) {
                let parts: Vec<&str> = credentials_str.split(':').collect();
                if parts.len() == 2 {
                    let (username, password) = (parts[0], parts[1]);
                    return username == self.rpc_user && password == self.rpc_password;
                }
            }
        }
        false
    }
}

#[async_trait]
impl<EF: ExecutorFactory> SolisApiServer for SolisApi<EF> {
    async fn set_addresses(
        &self,
        addresses: HookerAddresses,
        basic_auth: String,
    ) -> Result<(), Error> {
        if !self.verify_basic_auth(&basic_auth) {
            panic!("authentication failed");
        }

        let mut hooker = self.sequencer.hooker.write().await;
        hooker.set_addresses(addresses);
        Ok(())
    }
}
