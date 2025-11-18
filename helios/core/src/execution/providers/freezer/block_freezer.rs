use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy::consensus::BlockHeader;
use tempfile::TempDir;
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::network::{primitives::HeaderResponse, BlockResponse};
use eyre::{eyre, Context, Result};
use futures::future::join_all;
use rayon::prelude::*;
use libmdbx::orm::{Database, DatabaseChart, table, table_info};
use std::sync::LazyLock;
use tracing::{debug, error, info, warn};

use helios_common::{
    execution_provider::{AccountProvider, BlockProvider},
    network_spec::NetworkSpec,
};

use crate::execution::providers::historical::HistoricalBlockProvider;

table!(
    /// Set of finalized block hashes - acts as a hash set for existence checks
    ( FinalizedBlockHashes ) [u8; 32] => ()
);

// Assemble database chart
static BLOCK_FREEZER_TABLES: LazyLock<Arc<DatabaseChart>> =
    LazyLock::new(|| Arc::new([table_info!(FinalizedBlockHashes)].into_iter().collect()));

/// Storage path for the block freezer - either persistent or temporary
enum StoragePath {
    /// Persistent path provided by user
    Persistent(PathBuf),
    /// Temporary directory that will be cleaned up on drop
    Temporary(TempDir),
}

impl StoragePath {
    fn path(&self) -> &Path {
        match self {
            StoragePath::Persistent(p) => p.as_path(),
            StoragePath::Temporary(t) => t.path(),
        }
    }
}

/// Block freezer that stores finalized block hashes in MDBX for long-term archival.
///
/// The freezer maintains a hash set of block hashes in MDBX and tracks the most recent
/// archived block via a `last_block.rlp` file. It continuously backfills historical blocks
/// and tracks new finalized blocks from the consensus client.
#[allow(dead_code)]
pub struct BlockFreezer<N: NetworkSpec, H: HistoricalBlockProvider<N>> {
    db: Arc<Database>,
    historical: Arc<H>,
    _storage_path: StoragePath,
    last_block_path: PathBuf,
    archive_from: u64,
    backfill_started: AtomicBool,
    _phantom: PhantomData<N>,
}

#[allow(dead_code)]
impl<N: NetworkSpec, H: HistoricalBlockProvider<N>> BlockFreezer<N, H> {
    /// Create a new BlockFreezer without starting backfill
    pub fn new(
        archive_from: u64,
        path: Option<PathBuf>,
        historical: H,
    ) -> Self
    {
        let historical = Arc::new(historical);
        // Create storage path - either persistent or temporary
        let storage_path = match path {
            Some(p) => StoragePath::Persistent(p),
            None => {
                let temp_dir = TempDir::new().expect("Failed to create temporary directory");
                info!("Using temporary storage at: {:?}", temp_dir.path());
                StoragePath::Temporary(temp_dir)
            }
        };

        let base_path = storage_path.path();
        let db_path = base_path.join("block_freezer");
        let last_block_path = base_path.join("last_block.rlp");

        let last_block_number = match load_last_block_number(&last_block_path) {
            Ok(num) => {
                info!("Loaded last block pointer from {:?}, block number: {}", last_block_path, num);
                Some(num)
            }
            Err(e) => {
                warn!("Failed to load last block pointer: {}. Will perform full resync.", e);
                None
            }
        };

        if last_block_number.is_none() {
            if let Err(e) = delete_db_directory(&db_path) {
                error!("Failed to delete DB directory: {}", e);
            }
        }

        let db = Arc::new(
            libmdbx::orm::Database::create(Some(db_path), &BLOCK_FREEZER_TABLES)
                .expect("Failed to create MDBX database"),
        );

        Self {
            db,
            historical,
            _storage_path: storage_path,
            last_block_path,
            archive_from,
            backfill_started: AtomicBool::new(false),
            _phantom: PhantomData,
        }
    }

    pub fn start_sync<E>(&self, execution: Arc<E>) -> Result<()>
    where
        E: BlockProvider<N> + AccountProvider<N> + Send + Sync + 'static,
    {
        if self.backfill_started.swap(true, Ordering::SeqCst) {
            return Err(eyre!("Backfill has already been started"));
        }
        
        let last_block_number = match load_last_block_number(&self.last_block_path) {
            Ok(num) => {
                info!("Loaded last stored block number for backfill: {}", num);
                Some(num)
            }
            Err(e) => {
                debug!("No last stored block number found for backfill: {}", e);
                None
            }
        };

        // Spawn backfill manager task
        let db_clone = Arc::clone(&self.db);
        let historical_clone = Arc::clone(&self.historical);
        let last_block_path = self.last_block_path.clone();
        let archive_from = self.archive_from;

        tokio::spawn(async move {
            if let Err(e) = backfill_manager_task(
                db_clone,
                historical_clone,
                execution,
                last_block_number,
                last_block_path,
                archive_from,
            )
            .await
            {
                error!("Backfill manager task failed: {}", e);
            }
        });

        info!("Backfill process started for archive_from: {}", archive_from);
        Ok(())
    }

    /// Get a block from the freezer. First queries the execution provider,
    /// then verifies the block exists in the local freezer database.
    /// 
    /// Returns None if:
    /// - The block doesn't exist in the execution provider
    /// - The block exists but hasn't been archived in the freezer yet
    pub async fn get_block<E>(
        &self,
        block_id: BlockId,
        full_tx: bool,
        execution: &E,
    ) -> Result<Option<N::BlockResponse>>
    where
        E: BlockProvider<N> + AccountProvider<N>,
    {
        let Some(untrusted_block) = execution.get_untrusted_block(block_id, full_tx).await? else {
            return Ok(None);
        };

        if !N::is_hash_valid(&untrusted_block) {
            return Err(eyre!("Fradulent block detected for block ID {:?}", block_id));
        }

        let block_hash: [u8; 32] = untrusted_block.header().hash().into();

        if block_hash_exists_in_freezer(Arc::clone(&self.db), block_hash).await? {
            Ok(Some(untrusted_block))
        } else {
            // Block exists in execution layer but not yet archived
            Ok(None)
        }
    }
}


/// Task 2: Backfill manager that processes finalized blocks and fills gaps
async fn backfill_manager_task<N, H, E>(
    db: Arc<Database>,
    historical: Arc<H>,
    execution: Arc<E>,
    last_block_number: Option<u64>,
    last_block_path: PathBuf,
    archive_from: u64,
) -> Result<()>
where
    N: NetworkSpec,
    H: HistoricalBlockProvider<N>,
    E: BlockProvider<N> + AccountProvider<N>,
{
    let mut last_stored_block: Option<N::BlockResponse> = if let Some(block_num) = last_block_number {
        match execution
            .get_untrusted_block(BlockId::number(block_num), false)
            .await
        {
            Ok(Some(block)) => {
                info!("Initialized from disk with block {}", block_num);
                Some(block)
            }
            Ok(None) => {
                warn!("Block {} from disk not found in execution layer, will resync", block_num);
                None
            }
            Err(e) => {
                warn!("Failed to fetch block {} from disk: {}, will resync", block_num, e);
                None
            }
        }
    } else {
        None
    };

    // Backfill strategy: query freezer_tip, backfill from last_stored_block (or archive_from) up to freezer_tip
    // Re-query freezer_tip every 60 seconds and repeat
    
    let mut retry_delay = Duration::from_secs(1);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
    const BATCH_SIZE: u64 = 32;

    loop {
        let freezer_tip = match get_freezer_tip::<N, H, E>(&historical, &execution).await {
            Ok(tip) => tip,
            Err(e) => {
                warn!("Failed to get freezer tip: {}. Retrying in {:?}", e, retry_delay);
                tokio::time::sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                continue;
            }
        };

        match process_backfill(
            &db,
            &execution,
            freezer_tip,
            &mut last_stored_block,
            &last_block_path,
            archive_from,
            BATCH_SIZE,
        )
        .await
        {
            Ok(()) => {
                // Success - reset retry delay and sleep for 60 seconds
                retry_delay = Duration::from_secs(1);
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Err(e) => {
                // Network error - exponential backoff
                warn!("Backfill error: {}. Retrying in {:?}", e, retry_delay);
                tokio::time::sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
            }
        }
    }
}

/// Process backfill logic - backfill backwards from freezer_tip to last_stored_block (or archive_from)
/// Uses freezer_tip as root of trust and verifies parent chain backwards
async fn process_backfill<N, E>(
    db: &Arc<Database>,
    execution: &Arc<E>,
    freezer_tip: N::BlockResponse,
    last_stored_block: &mut Option<N::BlockResponse>,
    last_block_path: &PathBuf,
    archive_from: u64,
    batch_size: u64,
) -> Result<()>
where
    N: NetworkSpec,
    E: BlockProvider<N> + AccountProvider<N>,
{
    let lower_exclusive = match last_stored_block {
        Some(block) => block.header().number(),
        None => archive_from.saturating_sub(1),
    };
    let lower_inclusive = lower_exclusive + 1;
    let freezer_tip_number = freezer_tip.header().number();

    if freezer_tip_number <= lower_exclusive {
        debug!("Caught up at block {}", lower_exclusive);
        return Ok(());
    }

    let freezer_tip_hash: [u8; 32] = freezer_tip.header().hash().into();
    store_block_hashes(Arc::clone(db), vec![freezer_tip_hash]).await?;
    debug!("Stored freezer tip at block {}", freezer_tip_number);

    let mut expected_parent_hash = freezer_tip.header().parent_hash();
    
    let start_time = tokio::time::Instant::now();
    let mut last_log_time = start_time;
    let mut total_blocks_processed = 1u64;
    let total_blocks_to_process = freezer_tip_number - lower_exclusive;
    
    let mut current_end = freezer_tip_number - 1;
    
    while current_end > lower_exclusive {
        let start = std::cmp::max(current_end.saturating_sub(batch_size - 1), lower_inclusive);
        
        debug!("Backfilling backwards from {} to {}", start, current_end);

        // Fetch and verify blocks
        let blocks = fetch_and_verify_blocks::<N, E>(execution, start, current_end).await?;
        
        // Verify the last block (highest number) matches our expected parent
        if let Some(last_block) = blocks.last() {
            let last_block_hash = last_block.header().hash();
            if last_block_hash != expected_parent_hash {
                return Err(eyre!(
                    "Parent hash mismatch at block {}: expected {}, got {}",
                    last_block.header().number(),
                    expected_parent_hash,
                    last_block_hash
                ));
            }
            
            if let Some(first_block) = blocks.first() {
                expected_parent_hash = first_block.header().parent_hash();
            }
        }
        
        let hashes: Vec<[u8; 32]> = blocks.iter().map(|b| b.header().hash().into()).collect();
        store_block_hashes(Arc::clone(db), hashes).await?;
        
        total_blocks_processed += blocks.len() as u64;
        
        let now = tokio::time::Instant::now();
        if now.duration_since(last_log_time) >= Duration::from_secs(5) {
            let elapsed = now.duration_since(start_time).as_secs_f64();
            let blocks_per_sec = total_blocks_processed as f64 / elapsed;
            let progress_percent = (total_blocks_processed as f64 / total_blocks_to_process as f64) * 100.0;
            
            info!(
                "Freezer sync progress: {}/{} blocks ({:.1}%) | {:.1} blocks/sec | current: {}",
                total_blocks_processed,
                total_blocks_to_process,
                progress_percent,
                blocks_per_sec,
                current_end
            );
            
            last_log_time = now;
        }
        
        current_end = start - 1;
    }

    save_last_block_number(last_block_path, freezer_tip_number)?;
    *last_stored_block = Some(freezer_tip);

    info!(
        "Freezer sync complete: verified chain from block {} down to {}",
        freezer_tip_number,
        lower_inclusive
    );
    Ok(())
}

/// Load last stored block number from disk
fn load_last_block_number(path: &Path) -> Result<u64> {
    let bytes = std::fs::read(path).context("Failed to read last block header file")?;
    if bytes.len() != 8 {
        return Err(eyre!("Invalid last block file length: expected 8 bytes, got {}", bytes.len()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Save last stored block number to disk atomically
fn save_last_block_number(path: &Path, number: u64) -> Result<()> {
    use std::io::Write;

    let encoded = number.to_be_bytes();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let temp_path = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temp_path)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    drop(file);

    std::fs::rename(&temp_path, path)?;
    Ok(())
}

/// Delete DB directory recursively
fn delete_db_directory(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)?;
        info!("Deleted DB directory at {:?}", path);
    }
    Ok(())
}

fn block_hash_exists_in_freezer_sync(db: &Database, block_hash: [u8; 32]) -> Result<bool> {
    let tx = db.begin_read()
        .map_err(|e| eyre!("Failed to begin read transaction: {}", e))?;
    
    let exists = tx.get::<FinalizedBlockHashes>(block_hash)
        .map_err(|e| eyre!("Failed to query freezer database: {}", e))?
        .is_some();
    
    Ok(exists)
}

async fn block_hash_exists_in_freezer(db: Arc<Database>, block_hash: [u8; 32]) -> Result<bool> {
    tokio::task::spawn_blocking(move || {
        block_hash_exists_in_freezer_sync(&db, block_hash)
    })
    .await
    .map_err(|e| eyre!("Task join error: {}", e))?
}

fn store_block_hashes_sync(db: &Database, block_hashes: &[[u8; 32]]) -> Result<()> {
    let tx = db.begin_readwrite().map_err(|e| eyre!("Failed to begin transaction: {}", e))?;
    
    for hash in block_hashes {
        tx.upsert::<FinalizedBlockHashes>(*hash, ())
            .map_err(|e| eyre!("Failed to upsert value: {}", e))?;
    }
    
    tx.commit().map_err(|e| eyre!("Failed to commit transaction: {}", e))?;
    
    Ok(())
}

async fn store_block_hashes(db: Arc<Database>, block_hashes: Vec<[u8; 32]>) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        store_block_hashes_sync(&db, &block_hashes)
    })
    .await
    .map_err(|e| eyre!("Task join error: {}", e))??;
    
    Ok(())
}

async fn fetch_and_verify_blocks<N, E>(
    execution: &Arc<E>,
    start: u64,
    end: u64,
) -> Result<Vec<N::BlockResponse>>
where
    N: NetworkSpec,
    E: BlockProvider<N> + AccountProvider<N>,
{
    let blocks = fetch_batch_blocks::<N, E>(execution, start, end).await?;

    let blocks = tokio::task::spawn_blocking(move || -> Result<Vec<N::BlockResponse>> {
        verify_parent_chain::<N>(&blocks)?;

        blocks.par_iter().try_for_each(|block| {
            if !N::is_hash_valid(block) {
                Err(eyre!("Block hash invalid: {}", block.header().hash()))
            } else {
                Ok(())
            }
        })?;

        Ok(blocks)
    })
    .await??;

    Ok(blocks)
}

/// Fetch a batch of blocks in parallel
async fn fetch_batch_blocks<N, E>(
    execution: &Arc<E>,
    start: u64,
    end: u64,
) -> Result<Vec<N::BlockResponse>>
where
    N: NetworkSpec,
    E: BlockProvider<N> + AccountProvider<N>,
{
    let mut futures = Vec::new();

    for block_num in start..=end {
        let execution_clone = Arc::clone(execution);
        let future = async move {
            let block_id = BlockId::Number(BlockNumberOrTag::Number(block_num));
            execution_clone
                .get_untrusted_block(block_id, false)
                .await
                .map(|opt| (block_num, opt))
        };
        futures.push(future);
    }

    let results = join_all(futures).await;

    let mut blocks = Vec::new();
    for result in results {
        let (block_num, block_opt) = result?;
        let block = block_opt.ok_or_else(|| eyre!("Block {} not found", block_num))?;
        blocks.push(block);
    }

    Ok(blocks)
}

/// Verify that blocks form a valid parent chain
fn verify_parent_chain<N>(blocks: &[N::BlockResponse]) -> Result<()>
where
    N: NetworkSpec,
{
    for i in 1..blocks.len() {
        let prev_hash = blocks[i - 1].header().hash();
        let curr_parent = blocks[i].header().parent_hash();

        if prev_hash != curr_parent {
                return Err(eyre!(
                "Parent chain verification failed at block {}: expected parent {}, got {}",
                blocks[i].header().number(),
                prev_hash,
                curr_parent
                ));
            }
        }

    Ok(())
}


async fn get_freezer_tip<N, H, E>(
    historical: &Arc<H>,
    execution: &Arc<E>,
) -> Result<N::BlockResponse>
where
    N: NetworkSpec,
    H: HistoricalBlockProvider<N>,
    E: BlockProvider<N> + AccountProvider<N>,
{
    let Some(latest_block) = execution.get_block(BlockId::Number(BlockNumberOrTag::Latest), false).await? else {
        return Err(eyre!("failed to get latest block"));
    };
    // Use latest - 1000 blocks as finalized proxy (OP Stack/Linea don't populate finalized_block_recv)
    let finalized_block_number = latest_block.header().number().saturating_sub(1000);
    let finalized_block = BlockId::Number(BlockNumberOrTag::Number(finalized_block_number));
    let Some(finalized_block) = historical.get_historical_block(finalized_block, false, execution.as_ref()).await? else {
        return Err(eyre!("failed to get finalized block"));
    };

    Ok(finalized_block)
}
