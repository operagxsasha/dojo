use std::sync::Arc;

use jsonrpsee::core::{async_trait, Error};
use katana_core::sequencer::KatanaSequencer;
use katana_executor::ExecutorFactory;
use katana_rpc_api::katana::SolisApiServer;
use katana_rpc_types::account::Account;

pub struct SolisApi<EF: ExecutorFactory> {
    sequencer: Arc<KatanaSequencer<EF>>,
}

impl<EF: ExecutorFactory> SolisApi<EF> {
    pub fn new(sequencer: Arc<KatanaSequencer<EF>>) -> Self {
        Self { sequencer }
    }
    
    fn verify_basic_auth(&self, encoded_credentials: &str) -> bool {
        if let Ok(credentials) = decode(encoded_credentials) {
            if let Ok(credentials_str) = String::from_utf8(credentials) {
                let parts: Vec<&str> = credentials_str.split(':').collect();
                if parts.len() == 2 {
                    let (username, password) = (parts[0], parts[1]);
                    return username == self.config.rpc_user
                        && password == self.config.rpc_password;
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

        self.sequencer.set_addresses(addresses).await;
        Ok(())
    }
}
