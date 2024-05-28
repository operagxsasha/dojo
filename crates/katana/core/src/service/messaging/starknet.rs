use crate::hooker::KatanaHooker;
use anyhow::Result;
use async_trait::async_trait;
use katana_primitives::chain::ChainId;
use katana_primitives::receipt::MessageToL1;
use katana_primitives::transaction::L1HandlerTx;
use katana_primitives::utils::transaction::compute_l1_message_hash;
use starknet::accounts::{Account, Call, ExecutionEncoding, SingleOwnerAccount};
use starknet::core::types::{BlockId, BlockTag, EmittedEvent, EventFilter, FieldElement};
use starknet::core::utils::starknet_keccak;
use starknet::macros::{felt, selector};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{AnyProvider, JsonRpcClient, Provider};
use starknet::signers::{LocalWallet, SigningKey};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{debug, error, info, trace, warn};
use url::Url;
use std::collections::HashSet;

use super::{Error, MessagingConfig, Messenger, MessengerResult, LOG_TARGET};

const MSG_MAGIC: FieldElement = felt!("0x4d5347");
const EXE_MAGIC: FieldElement = felt!("0x455845");

pub const HASH_EXEC: FieldElement = felt!("0xee");

pub struct EventCache {
    processed_events: AsyncRwLock<HashSet<String>>,
}

impl EventCache {
    pub fn new() -> Self {
        EventCache {
            processed_events: AsyncRwLock::new(HashSet::new()),
        }
    }

    pub async fn is_event_processed(&self, event_id: &String) -> bool {
        let events = self.processed_events.read().await;
        events.contains(event_id)
    }

    pub async fn mark_event_as_processed(&self, event_id: String) {
        let mut events = self.processed_events.write().await;
        events.insert(event_id);
    }

    pub async fn clear(&self) {
        let mut events = self.processed_events.write().await;
        events.clear();
    }
}

pub struct StarknetMessaging<EF: katana_executor::ExecutorFactory + Send + Sync> {
    chain_id: FieldElement,
    provider: AnyProvider,
    wallet: LocalWallet,
    sender_account_address: FieldElement,
    messaging_contract_address: FieldElement,
    hooker: Arc<AsyncRwLock<dyn KatanaHooker<EF> + Send + Sync>>,
    cache_lock: AsyncRwLock<()>,
    event_cache: Arc<EventCache>,
}

impl<EF: katana_executor::ExecutorFactory + Send + Sync> StarknetMessaging<EF> {
    pub async fn new(
        config: MessagingConfig,
        hooker: Arc<AsyncRwLock<dyn KatanaHooker<EF> + Send + Sync>>,
    ) -> Result<StarknetMessaging<EF>> {
        let provider = AnyProvider::JsonRpcHttp(JsonRpcClient::new(HttpTransport::new(
            Url::parse(&config.rpc_url)?,
        )));

        let private_key = FieldElement::from_hex_be(&config.private_key)?;
        let key = SigningKey::from_secret_scalar(private_key);
        let wallet = LocalWallet::from_signing_key(key);

        let chain_id = provider.chain_id().await?;
        let sender_account_address = FieldElement::from_hex_be(&config.sender_address)?;
        let messaging_contract_address = FieldElement::from_hex_be(&config.contract_address)?;

        info!(target: LOG_TARGET, "StarknetMessaging instance created.");

        Ok(StarknetMessaging {
            wallet,
            provider,
            chain_id,
            sender_account_address,
            messaging_contract_address,
            hooker,
            cache_lock: AsyncRwLock::new(()),
            event_cache: Arc::new(EventCache::new()),
        })
    }

    /// Fetches events for the given blocks range.
    pub async fn fetch_events(
        &self,
        from_block: BlockId,
        to_block: BlockId,
    ) -> Result<HashMap<u64, Vec<EmittedEvent>>> {
        trace!(target: LOG_TARGET, from_block = ?from_block, to_block = ?to_block, "Fetching logs.");

        let mut block_to_events: HashMap<u64, Vec<EmittedEvent>> = HashMap::new();

        let filter = EventFilter {
            from_block: Some(from_block),
            to_block: Some(to_block),
            address: Some(self.messaging_contract_address),
            keys: None,
        };

        let chunk_size = 200;
        let mut continuation_token: Option<String> = None;

        loop {
            let event_page =
                self.provider.get_events(filter.clone(), continuation_token, chunk_size).await?;

            event_page.events.into_iter().for_each(|event| {
                if let Some(block_number) = event.block_number {
                    block_to_events
                        .entry(block_number)
                        .and_modify(|v| v.push(event.clone()))
                        .or_insert(vec![event]);
                }
            });

            continuation_token = event_page.continuation_token;

            if continuation_token.is_none() {
                break;
            }
        }

        Ok(block_to_events)
    }

    async fn fetch_pending_events(
        &self,
        chain_id: ChainId,
        chunk_size: u64,
    ) -> Result<Vec<L1HandlerTx>> {
        let mut l1_handler_txs: Vec<L1HandlerTx> = vec![];
        let mut continuation_token: Option<String> = None;

        loop {
            let filter = EventFilter {
                from_block: Some(BlockId::Tag(BlockTag::Pending)),
                to_block: Some(BlockId::Tag(BlockTag::Pending)),
                address: Some(self.messaging_contract_address),
                keys: None,
            };

            let event_page = self.provider.get_events(filter.clone(), continuation_token.clone(), chunk_size).await?;

            for event in event_page.events {
                let event_id = event.transaction_hash.to_string(); // Assuming `transaction_hash` is the unique identifier for the event

                if self.event_cache.is_event_processed(&event_id).await {
                    continue;
                }

                if let Ok(tx) = l1_handler_tx_from_event(&event, chain_id) {
                    if let Ok((from, to, selector)) = info_from_event(&event) {
                        let hooker = Arc::clone(&self.hooker);
                        let is_message_accepted = hooker
                            .read()
                            .await
                            .verify_message_to_appchain(from, to, selector)
                            .await;

                        if is_message_accepted {
                            l1_handler_txs.push(tx);
                            self.event_cache.mark_event_as_processed(event_id).await;
                        }
                    }
                }
            }

            continuation_token = event_page.continuation_token;

            if continuation_token.is_none() {
                break;
            }
        }

        Ok(l1_handler_txs)
    }

    async fn send_invoke_tx(&self, calls: Vec<Call>) -> Result<FieldElement> {
        let signer = Arc::new(&self.wallet);

        let mut account = SingleOwnerAccount::new(
            &self.provider,
            signer,
            self.sender_account_address,
            self.chain_id,
            ExecutionEncoding::New,
        );

        account.set_block_id(BlockId::Tag(BlockTag::Pending));

        let execution = account.execute(calls).fee_estimate_multiplier(10f64);
        let estimated_fee = (execution.estimate_fee().await?.overall_fee) * 10u64.into();
        let execution_with_fee = execution.max_fee(estimated_fee);

        info!(target: LOG_TARGET, "Sending invoke transaction.");

        match execution_with_fee.send().await {
            Ok(tx) => {
                info!(target: LOG_TARGET, "Transaction successful: {:?}", tx);
                println!("tx: {:?}", tx);
                println!("tx_hash: {:?}", tx.transaction_hash);
                Ok(tx.transaction_hash)
            }
            Err(e) => {
                error!(target: LOG_TARGET, "Error sending transaction: {:?}", e);
                Err(e.into())
            }
        }
    }

    async fn send_hashes(&self, mut hashes: Vec<FieldElement>) -> MessengerResult<FieldElement> {
        hashes.retain(|&x| x != HASH_EXEC);

        if hashes.is_empty() {
            info!(target: LOG_TARGET, "No hashes to send.");
            return Ok(FieldElement::ZERO);
        }

        let mut calldata = hashes;
        calldata.insert(0, calldata.len().into());

        let call = Call {
            selector: selector!("add_messages_hashes_from_appchain"),
            to: self.messaging_contract_address,
            calldata,
        };

        info!(target: LOG_TARGET, "Sending hashes to Starknet.");

        match self.send_invoke_tx(vec![call]).await {
            Ok(tx_hash) => {
                trace!(target: LOG_TARGET, tx_hash = %format!("{:#064x}", tx_hash), "Hashes sending transaction.");
                Ok(tx_hash)
            }
            Err(e) => {
                error!(target: LOG_TARGET, error = %e, "Settling hashes on Starknet.");
                Err(Error::SendError)
            }
        }
    }
}

#[async_trait]
impl<EF: katana_executor::ExecutorFactory + Send + Sync> Messenger for StarknetMessaging<EF> {
    type MessageHash = FieldElement;
    type MessageTransaction = L1HandlerTx;

    async fn gather_messages(
        &self,
        from_block: u64,
        max_blocks: u64,
        chain_id: ChainId,
    ) -> MessengerResult<(u64, Vec<Self::MessageTransaction>)> {
        let chain_latest_block: u64 = match self.provider.block_number().await {
            Ok(n) => n,
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    "Couldn't fetch settlement chain last block number. Skipped, retry at the next tick. Error: {:?}", e
                );
                return Err(Error::SendError);
            }
        };

        if from_block > chain_latest_block {
            // Nothing to fetch, we can skip waiting for the next tick.
            return Ok((chain_latest_block, vec![]));
        }

        // +1 as the from_block counts as 1 block fetched.
        let to_block = if from_block + max_blocks + 1 < chain_latest_block {
            from_block + max_blocks
        } else {
            chain_latest_block
        };

        let mut l1_handler_txs: Vec<L1HandlerTx> = vec![];

        info!(target: LOG_TARGET, "Gathering messages from block {} to block {}", from_block, to_block);

        let block_to_events = self
            .fetch_events(BlockId::Number(from_block), BlockId::Number(to_block))
            .await
            .map_err(|e| {
                error!(target: LOG_TARGET, "Error fetching events: {:?}", e);
                Error::SendError
            })?;

        for block_events in block_to_events.values() {
            for event in block_events {
                if let Ok(tx) = l1_handler_tx_from_event(event, chain_id) {
                    if let Ok((from, to, selector)) = info_from_event(event) {
                        let hooker = Arc::clone(&self.hooker);
                        let is_message_accepted = hooker
                            .read()
                            .await
                            .verify_message_to_appchain(from, to, selector)
                            .await;

                        if is_message_accepted {
                            l1_handler_txs.push(tx);
                        }
                    }
                }
            }
        }

        // Now, handle pending block events
        {
            // Use a lock to ensure atomicity
            let cache_lock = self.cache_lock.write().await;

            // Fetch pending block events
            let pending_events = self.fetch_pending_events(chain_id, 100).await.map_err(|e| {
                error!(target: LOG_TARGET, "Error fetching pending events: {:?}", e);
                Error::SendError
            })?;
            l1_handler_txs.extend(pending_events);

            // Get the latest block number again to ensure we didn't miss any new blocks
            let latest_block_number = match self.provider.block_number().await {
                Ok(n) => n,
                Err(e) => {
                    warn!(
                        target: LOG_TARGET,
                        "Couldn't fetch settlement chain last block number. Skipped, retry at the next tick. Error: {:?}", e
                    );
                    return Err(Error::SendError);
                }
            };

            // Fetch all events from the latest block to ensure none are missed
            let confirmed_events = self.fetch_events(BlockId::Number(latest_block_number), BlockId::Number(latest_block_number)).await.map_err(|e| {
                error!(target: LOG_TARGET, "Error fetching confirmed block events: {:?}", e);
                Error::SendError
            })?;
            
            for block_events in confirmed_events.values() {
                for event in block_events {
                    let event_id = event.transaction_hash.to_string();
                    if !self.event_cache.is_event_processed(&event_id).await {
                        if let Ok(tx) = l1_handler_tx_from_event(event, chain_id) {
                            if let Ok((from, to, selector)) = info_from_event(event) {
                                let hooker = Arc::clone(&self.hooker);
                                let is_message_accepted = hooker
                                    .read()
                                    .await
                                    .verify_message_to_appchain(from, to, selector)
                                    .await;

                                if is_message_accepted {
                                    l1_handler_txs.push(tx);
                                }
                            }
                        }
                    }
                }
            }

            self.event_cache.clear().await;

            // Fetch pending events again to ensure no events were missed during the cache clearing
            let rechecked_pending_events = self.fetch_pending_events(chain_id, 100).await.map_err(|e| {
                error!(target: LOG_TARGET, "Error rechecking pending events: {:?}", e);
                Error::SendError
            })?;
            l1_handler_txs.extend(rechecked_pending_events);

            drop(cache_lock); // Release the lock
        }

        Ok((to_block, l1_handler_txs))
    }

    async fn send_messages(
        &self,
        messages: &[MessageToL1],
    ) -> MessengerResult<Vec<<Self as Messenger>::MessageHash>> {
        if messages.is_empty() {
            info!(target: LOG_TARGET, "No messages to send.");
            return Ok(vec![]);
        }

        let (hashes, calls) = parse_messages(messages)?;

        for call in &calls {
            if !self.hooker.read().await.verify_tx_for_starknet(call.clone()).await {
                warn!(target: LOG_TARGET, "Call verification failed for call: {:?}", call);
                continue;
            }
        }

        if !calls.is_empty() {
            info!(target: LOG_TARGET, "Sending invoke transactions for calls.");
            match self.send_invoke_tx(calls.clone()).await {
                Ok(tx_hash) => {
                    trace!(target: LOG_TARGET, tx_hash = %format!("{:#064x}", tx_hash), "Invoke transaction hash.");
                }
                Err(e) => {
                    error!(target: LOG_TARGET, error = %e, "Sending invoke tx on Starknet.");
                    for call in calls {
                        self.hooker.read().await.on_starknet_tx_failed(call).await;
                    }
                    return Err(Error::SendError);
                }
            };
        }

        self.send_hashes(hashes.clone()).await?;

        Ok(hashes)
    }
}

fn parse_messages(messages: &[MessageToL1]) -> MessengerResult<(Vec<FieldElement>, Vec<Call>)> {
    let mut hashes: Vec<FieldElement> = vec![];
    let mut calls: Vec<Call> = vec![];

    for m in messages {
        let magic = m.to_address;

        if magic == EXE_MAGIC {
            if m.payload.len() < 2 {
                error!(
                    target: LOG_TARGET,
                    "Message execution is expecting a payload of at least length \
                     2. With [0] being the contract address, and [1] the selector.",
                );
                continue;
            }

            let to = m.payload[0];
            let selector = m.payload[1];

            let mut calldata = vec![];
            if m.payload.len() >= 3 {
                calldata.extend(m.payload[2..].to_vec());
            }

            calls.push(Call { to, selector, calldata });
            hashes.push(HASH_EXEC);
        } else if magic == MSG_MAGIC {
            let to_address = m.payload[0];
            let payload = &m.payload[1..];

            let mut buf: Vec<u8> = vec![];
            buf.extend(m.from_address.to_bytes_be());
            buf.extend(to_address.to_bytes_be());
            buf.extend(FieldElement::from(payload.len()).to_bytes_be());
            for p in payload {
                buf.extend(p.to_bytes_be());
            }

            hashes.push(starknet_keccak(&buf));
        } else {
            warn!(target: LOG_TARGET, magic = ?magic, "Invalid message to_address magic value.");
            continue;
        }
    }

    Ok((hashes, calls))
}

fn l1_handler_tx_from_event(event: &EmittedEvent, chain_id: ChainId) -> Result<L1HandlerTx> {
    if event.keys[0] != selector!("MessageSentToAppchain") {
        debug!(
            target: LOG_TARGET,
            event_key = ?event.keys[0],
            "Event can't be converted into L1HandlerTx."
        );
        return Err(Error::GatherError.into());
    }

    if event.keys.len() != 4 || event.data.len() < 2 {
        error!(target: LOG_TARGET, "Event MessageSentToAppchain is not well formatted.");
    }

    let from_address = event.keys[2];
    let to_address = event.keys[3];
    let entry_point_selector = event.data[0];
    let nonce = event.data[1];

    let mut calldata = vec![from_address];
    calldata.extend(&event.data[3..]);

    let message_hash = compute_l1_message_hash(from_address, to_address, &calldata);

    Ok(L1HandlerTx {
        nonce,
        calldata,
        chain_id,
        message_hash,
        paid_fee_on_l1: 30000_u128,
        entry_point_selector,
        version: FieldElement::ZERO,
        contract_address: to_address.into(),
    })
}

fn info_from_event(event: &EmittedEvent) -> Result<(FieldElement, FieldElement, FieldElement)> {
    if event.keys[0] != selector!("MessageSentToAppchain") {
        debug!(
            target: LOG_TARGET,
            "Event with key {:?} can't be converted into L1HandlerTx", event.keys[0],
        );
        return Err(Error::GatherError.into());
    }

    if event.keys.len() != 4 || event.data.len() < 2 {
        error!(target: LOG_TARGET, "Event MessageSentToAppchain is not well formatted");
    }

    let from_address = event.keys[2];
    let to_address = event.keys[3];
    let entry_point_selector = event.data[0];

    Ok((from_address, to_address, entry_point_selector))
}

#[cfg(test)]
mod tests {

    use katana_primitives::utils::transaction::compute_l1_handler_tx_hash;
    use starknet::macros::felt;

    use super::*;

    #[test]
    fn parse_messages_msg() {
        let from_address = selector!("from_address");
        let to_address = selector!("to_address");
        let selector = selector!("selector");
        let payload_msg = vec![to_address, FieldElement::ONE, FieldElement::TWO];
        let payload_exe = vec![to_address, selector, FieldElement::ONE, FieldElement::TWO];

        let messages = vec![
            MessageToL1 {
                from_address: from_address.into(),
                to_address: MSG_MAGIC,
                payload: payload_msg,
            },
            MessageToL1 {
                from_address: from_address.into(),
                to_address: EXE_MAGIC,
                payload: payload_exe.clone(),
            },
        ];

        let (hashes, calls) = parse_messages(&messages).unwrap();

        assert_eq!(hashes.len(), 2);
        assert_eq!(
            hashes,
            vec![
                FieldElement::from_hex_be(
                    "0x03a1d2e131360f15e26dd4f6ff10550685611cc25f75e7950b704adb04b36162"
                )
                .unwrap(),
                HASH_EXEC,
            ]
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].to, to_address);
        assert_eq!(calls[0].selector, selector);
        assert_eq!(calls[0].calldata, payload_exe[2..].to_vec());
    }

    #[test]
    #[should_panic]
    fn parse_messages_msg_bad_payload() {
        let from_address = selector!("from_address");
        let payload_msg = vec![];

        let messages = vec![MessageToL1 {
            from_address: from_address.into(),
            to_address: MSG_MAGIC,
            payload: payload_msg,
        }];

        parse_messages(&messages).unwrap();
    }

    #[test]
    #[should_panic]
    fn parse_messages_exe_bad_payload() {
        let from_address = selector!("from_address");
        let payload_exe = vec![FieldElement::ONE];

        let messages = vec![MessageToL1 {
            from_address: from_address.into(),
            to_address: EXE_MAGIC,
            payload: payload_exe,
        }];

        parse_messages(&messages).unwrap();
    }

    #[test]
    fn l1_handler_tx_from_event_parse_ok() {
        let from_address = selector!("from_address");
        let to_address = selector!("to_address");
        let selector = selector!("selector");
        let chain_id = ChainId::parse("KATANA").unwrap();
        let nonce = FieldElement::ONE;
        let calldata = vec![from_address, FieldElement::THREE];

        let transaction_hash: FieldElement = compute_l1_handler_tx_hash(
            FieldElement::ZERO,
            to_address,
            selector,
            &calldata,
            chain_id.into(),
            nonce,
        );

        let event = EmittedEvent {
            from_address: felt!(
                "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
            ),
            keys: vec![
                selector!("MessageSentToAppchain"),
                selector!("random_hash"),
                from_address,
                to_address,
            ],
            data: vec![
                selector,
                nonce,
                FieldElement::from(calldata.len() as u128),
                FieldElement::THREE,
            ],
            block_hash: Some(selector!("block_hash")),
            block_number: Some(0),
            transaction_hash,
        };

        let message_hash = compute_l1_message_hash(from_address, to_address, &calldata);

        let expected = L1HandlerTx {
            nonce,
            calldata,
            chain_id,
            message_hash,
            paid_fee_on_l1: 30000_u128,
            entry_point_selector: selector,
            version: FieldElement::ZERO,
            contract_address: to_address.into(),
        };

        let tx = l1_handler_tx_from_event(&event, chain_id).unwrap();

        assert_eq!(tx, expected);
    }

    #[test]
    #[should_panic]
    fn l1_handler_tx_from_event_parse_bad_selector() {
        let from_address = selector!("from_address");
        let to_address = selector!("to_address");
        let selector = selector!("selector");
        let nonce = FieldElement::ONE;
        let calldata = [from_address, FieldElement::THREE];
        let transaction_hash = FieldElement::ZERO;

        let event = EmittedEvent {
            from_address: felt!(
                "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
            ),
            keys: vec![
                selector!("AnOtherUnexpectedEvent"),
                selector!("random_hash"),
                from_address,
                to_address,
            ],
            data: vec![
                selector,
                nonce,
                FieldElement::from(calldata.len() as u128),
                FieldElement::THREE,
            ],
            block_hash: Some(selector!("block_hash")),
            block_number: Some(0),
            transaction_hash,
        };

        let _tx = l1_handler_tx_from_event(&event, ChainId::default()).unwrap();
    }

    #[test]
    #[should_panic]
    fn l1_handler_tx_from_event_parse_missing_key_data() {
        let from_address = selector!("from_address");
        let _to_address = selector!("to_address");
        let _selector = selector!("selector");
        let _nonce = FieldElement::ONE;
        let _calldata = [from_address, FieldElement::THREE];
        let transaction_hash = FieldElement::ZERO;

        let event = EmittedEvent {
            from_address: felt!(
                "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
            ),
            keys: vec![selector!("AnOtherUnexpectedEvent"), selector!("random_hash"), from_address],
            data: vec![],
            block_hash: Some(selector!("block_hash")),
            block_number: Some(0),
            transaction_hash,
        };

        let _tx = l1_handler_tx_from_event(&event, ChainId::default()).unwrap();
    }
}
