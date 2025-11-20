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
use tokio::sync::mpsc;

use helios_common::{
    execution_provider::{AccountProvider, BlockProvider},
    network_spec::NetworkSpec,
};

use crate::execution::providers::historical::HistoricalBlockProvider;

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

const NETWORK_BATCH_SIZE: u64 = 10;
// Pipeline buffer size
const CHANNEL_BUFFER: usize = 16;

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
            execution.clone(),
            freezer_tip,
            &mut last_stored_block,
            &last_block_path,
            archive_from,
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
/// Implements a 3-stage pipeline: Downloader -> Verifier -> Persister
async fn process_backfill<N, E>(
    db: &Arc<Database>,
    execution: Arc<E>,
    freezer_tip: N::BlockResponse,
    last_stored_block: &mut Option<N::BlockResponse>,
    last_block_path: &PathBuf,
    archive_from: u64,
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
    
    // Pipeline Configuration
    let (downloader_tx, downloader_rx) = mpsc::channel(CHANNEL_BUFFER);
    let (verifier_tx, mut verifier_rx) = mpsc::channel(CHANNEL_BUFFER);

    // Stage 1: Downloader
    let execution_clone = execution.clone();
    let downloader_handle = tokio::spawn(async move {
        let result = spawn_downloader::<N, E>(
            execution_clone,
            lower_inclusive,
            freezer_tip_number - 1, // Start below tip
            NETWORK_BATCH_SIZE,
            downloader_tx
        ).await;
        if let Err(ref e) = result {
            error!("Downloader task error: {}", e);
        }
        result
    });

    // Stage 2: Verifier
    let verifier_handle = tokio::spawn(async move {
        let result = spawn_verifier::<N>(
            downloader_rx,
            verifier_tx
        ).await;
        if let Err(ref e) = result {
            error!("Verifier task error: {}", e);
        }
        result
    });

    // Stage 3: Persister (Main Loop)
    let mut pipeline_error = None;
    let mut persister_blocks_processed = 0u64;
    let mut persister_last_log_time = start_time;

    while let Some(blocks) = verifier_rx.recv().await {
        // Blocks are already sorted descending by the Verifier stage
        
        if let Some(first_block) = blocks.first() {
            let first_block_hash = first_block.header().hash();
            if first_block_hash != expected_parent_hash {
                pipeline_error = Some(eyre!(
                    "Parent hash mismatch at block {}: expected {}, got {}",
                    first_block.header().number(),
                    expected_parent_hash,
                    first_block_hash
                ));
                break;
            }
            
            // The LAST block in the vector is the LOWEST number, which points to the next parent we need
            if let Some(last_block) = blocks.last() {
                expected_parent_hash = last_block.header().parent_hash();
            }
        }

        let hashes: Vec<[u8; 32]> = blocks.iter().map(|b| b.header().hash().into()).collect();
        if let Err(e) = store_block_hashes(Arc::clone(db), hashes).await {
            pipeline_error = Some(e);
            break;
        }

        
        total_blocks_processed += blocks.len() as u64;
        persister_blocks_processed += blocks.len() as u64;
        
        let now = tokio::time::Instant::now();
        if now.duration_since(last_log_time) >= Duration::from_secs(1) {
            let elapsed = now.duration_since(start_time).as_secs_f64();
            let blocks_per_sec = total_blocks_processed as f64 / elapsed;
            let progress_percent = (total_blocks_processed as f64 / total_blocks_to_process as f64) * 100.0;
            
            info!(
                "Freezer backfill progress: {}/{} blocks ({:.1}%) | {:.1} blocks/sec",
                total_blocks_processed,
                total_blocks_to_process,
                progress_percent,
                blocks_per_sec,
            );
            
            last_log_time = now;
        }
        
        if now.duration_since(persister_last_log_time) >= Duration::from_secs(5) {
            let persister_elapsed = now.duration_since(start_time).as_secs_f64();
            let persister_rate = persister_blocks_processed as f64 / persister_elapsed;
            debug!("Persister: {:.1} blocks/sec", persister_rate);
            persister_last_log_time = now;
        }
    }

    // Check for errors from tasks or loop
    if let Some(e) = pipeline_error {
        // Abort tasks
        downloader_handle.abort();
        verifier_handle.abort();
        return Err(e);
    }
    
    // Check task results
    match downloader_handle.await {
        Ok(Ok(())) => debug!("Downloader completed successfully"),
        Ok(Err(e)) => return Err(e.wrap_err("Downloader returned error")),
        Err(e) if e.is_cancelled() => debug!("Downloader was cancelled"),
        Err(e) => return Err(eyre!("Downloader task panicked: {}", e)),
    }
    
    match verifier_handle.await {
        Ok(Ok(())) => debug!("Verifier completed successfully"),
        Ok(Err(e)) => return Err(e.wrap_err("Verifier returned error")),
        Err(e) if e.is_cancelled() => debug!("Verifier was cancelled"),
        Err(e) => return Err(eyre!("Verifier task panicked: {}", e)),
    }

    // Check if we actually processed everything
    // Note: total_blocks_processed starts at 1?
    // Wait, line 324: let mut total_blocks_processed = 1u64;
    // Why 1? Ah, maybe because we count the freezer_tip?
    // freezer_tip_number is inclusive. lower_exclusive is exclusive.
    // Count = freezer_tip_number - lower_exclusive.
    // We store freezer_tip at line 317. So that's 1 block.
    
    if total_blocks_processed < total_blocks_to_process {
         return Err(eyre!(
            "Pipeline ended prematurely: processed {}/{} blocks. Check logs for task errors.",
            total_blocks_processed,
            total_blocks_to_process
        ));
    }

    save_last_block_number(last_block_path, freezer_tip_number)?;
    *last_stored_block = Some(freezer_tip);

    info!(
        "Freezer backfill complete: verified chain from block {} to {}",
        freezer_tip_number,
        lower_inclusive
    );
    Ok(())
}

/// Pipeline Stage 1: Downloader
/// Fetches blocks backwards from end_block to start_block in batches
async fn spawn_downloader<N, E>(
    execution: Arc<E>,
    start_block: u64,
    end_block: u64,
    batch_size: u64,
    sender: mpsc::Sender<Vec<N::BlockResponse>>,
) -> Result<()>
where
    N: NetworkSpec,
    E: BlockProvider<N> + AccountProvider<N>,
{
    debug!("Downloader starting: start_block={}, end_block={}, batch_size={}", start_block, end_block, batch_size);
    let mut current_end = end_block;
    
    let start_time = tokio::time::Instant::now();
    let mut total_blocks_downloaded = 0u64;
    let mut last_log_time = start_time;
    
    loop {
        if current_end < start_block {
            debug!("Downloader finished: current_end={} < start_block={}", current_end, start_block);
            break;
        }
        
        let start = std::cmp::max(current_end.saturating_sub(batch_size - 1), start_block);
        debug!("Downloader: fetching blocks {} to {}", start, current_end);
        
        let mut retry_delay = Duration::from_secs(1);

        loop {
            match fetch_batch_blocks::<N, E>(&execution, start, current_end).await {
                Ok(blocks) => {
                    let block_count = blocks.len() as u64;
                    total_blocks_downloaded += block_count;
                    
                    let now = tokio::time::Instant::now();
                    if now.duration_since(last_log_time) >= Duration::from_secs(5) {
                        let elapsed = now.duration_since(start_time).as_secs_f64();
                        let download_rate = total_blocks_downloaded as f64 / elapsed;
                        debug!("Downloader: {:.1} blocks/sec", download_rate);
                        last_log_time = now;
                    }
                    
                    if sender.send(blocks).await.is_err() {
                        return Ok(()); // Receiver dropped
                    }
                    break; // Success, move to next batch
                }
                Err(e) => {
                    if retry_delay >= MAX_RETRY_DELAY {
                        return Err(e.wrap_err("Failed to fetch batch blocks after max retries"));
                    }
                    warn!("Downloader fetch error: {}. Retrying in {:?}", e, retry_delay);
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }
        
        if start == 0 { break; }
        current_end = start - 1;
    }
    
    let total_elapsed = tokio::time::Instant::now().duration_since(start_time).as_secs_f64();
    let final_rate = total_blocks_downloaded as f64 / total_elapsed;
    debug!("Downloader completed: {} blocks in {:.1}s ({:.1} blocks/sec)", 
        total_blocks_downloaded, total_elapsed, final_rate);
    
    Ok(())
}

/// Pipeline Stage 2: Verifier
/// Verifies blocks CPU-intensively on thread pool
async fn spawn_verifier<N>(
    mut receiver: mpsc::Receiver<Vec<N::BlockResponse>>,
    sender: mpsc::Sender<Vec<N::BlockResponse>>,
) -> Result<()>
where
    N: NetworkSpec,
{
    let start_time = tokio::time::Instant::now();
    let mut total_blocks_verified = 0u64;
    let mut last_log_time = start_time;
    
    while let Some(blocks) = receiver.recv().await {
        let block_count = blocks.len() as u64;
        
        let verified_blocks = tokio::task::spawn_blocking(move || -> Result<Vec<N::BlockResponse>> {
            // Sort descending to verify parent chain backwards
            let mut blocks = blocks;
            blocks.sort_by_key(|b| std::cmp::Reverse(b.header().number()));
            verify_parent_chain::<N>(&blocks)?;
            
            blocks.par_iter().try_for_each(|block| {
                if !N::is_hash_valid(block) {
                    Err(eyre!("Block hash invalid: {}", block.header().hash()))
                } else {
                    Ok(())
                }
            })?;
            
            Ok(blocks)
        }).await??;

        total_blocks_verified += block_count;
        
        let now = tokio::time::Instant::now();
        if now.duration_since(last_log_time) >= Duration::from_secs(5) {
            let elapsed = now.duration_since(start_time).as_secs_f64();
            let verify_rate = total_blocks_verified as f64 / elapsed;
            debug!("Verifier: {:.1} blocks/sec", verify_rate);
            last_log_time = now;
        }

        if sender.send(verified_blocks).await.is_err() {
            break; 
        }
    }
    
    let total_elapsed = tokio::time::Instant::now().duration_since(start_time).as_secs_f64();
    let final_rate = total_blocks_verified as f64 / total_elapsed;
    debug!("Verifier completed: {} blocks in {:.1}s ({:.1} blocks/sec)", 
        total_blocks_verified, total_elapsed, final_rate);
    
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

/// Fetch a batch of blocks in parallel, maintaining order
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

    // Collect with block numbers to ensure correct ordering
    let mut blocks_with_nums = Vec::new();
    for result in results {
        let (block_num, block_opt) = result?;
        let block = block_opt.ok_or_else(|| eyre!("Block {} not found", block_num))?;
        blocks_with_nums.push((block_num, block));
    }
    
    // Sort by block number to ensure ascending order
    blocks_with_nums.sort_by_key(|(num, _)| *num);
    
    let blocks: Vec<N::BlockResponse> = blocks_with_nums.into_iter().map(|(_, block)| block).collect();

    Ok(blocks)
}

/// Verify that blocks form a valid parent chain
/// Expects blocks in DESCENDING order (highest number first)
fn verify_parent_chain<N>(blocks: &[N::BlockResponse]) -> Result<()>
where
    N: NetworkSpec,
{
    for i in 1..blocks.len() {
        let curr_hash = blocks[i].header().hash();
        let prev_parent = blocks[i - 1].header().parent_hash();

        if curr_hash != prev_parent {
                return Err(eyre!(
                "Parent chain verification failed at block {}: block hash {}, but next block {} expects parent {}",
                blocks[i].header().number(),
                curr_hash,
                blocks[i - 1].header().number(),
                prev_parent
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
    // Use latest - 8191 / 2 blocks as finalized proxy (OP Stack/Linea don't populate finalized_block_recv)
    let finalized_block_number = latest_block.header().number().saturating_sub(8191/2);
    let finalized_block = BlockId::Number(BlockNumberOrTag::Number(finalized_block_number));
    let Some(finalized_block) = historical.get_historical_block(finalized_block, false, execution.as_ref()).await? else {
        return Err(eyre!("failed to get finalized block"));
    };

    Ok(finalized_block)
}
