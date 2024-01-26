use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::Parser;
use futures::FutureExt;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Keypair,
    signer::{EncodableKey, Signer},
};

#[macro_use]
extern crate log;

#[derive(Parser)]
#[command(author, version)]
pub struct Scraper {
    /// Which directories to look in
    paths: Vec<PathBuf>,
    /// How deep to go in each directory
    #[arg(long, short, default_value_t = 2)]
    depth: usize,
    /// Which RPC to use for balance/owner checking (can be specified multiple times)
    #[arg(long, short)]
    rpc: Vec<String>,
}

pub fn find_nested_dirs(p: &Path, remaining_levels: usize) -> Result<Vec<PathBuf>> {
    let mut ret = Vec::new();
    ret.push(p.to_owned());

    if remaining_levels == 0 {
        return Ok(ret);
    }

    let path_contents = std::fs::read_dir(p)?
        .filter_map(|r| r.ok())
        .filter(|entry| entry.path().is_dir());

    for entry in path_contents {
        let mut partial = match find_nested_dirs(&entry.path(), remaining_levels - 1) {
            Ok(nested_ok) => nested_ok,
            Err(e) => {
                trace!("Probably not a directory: {:?}", e.to_string());
                vec![]
            }
        };

        ret.append(&mut partial);
    }

    Ok(ret)
}

pub fn find_solana_keypairs(p: &Path) -> Result<Vec<(PathBuf, Pubkey)>> {
    let mut ret = Vec::new();
    let files = std::fs::read_dir(p)?
        .filter_map(|r| r.ok())
        .filter(|entry| entry.path().is_file());

    for file in files {
        match Keypair::read_from_file(file.path()) {
            Ok(kp) => {
                ret.push((file.path().to_owned(), kp.pubkey()));
            }
            Err(e) => {
                trace!("Probably not a keypair, {}", e.to_string());
            }
        }
    }

    Ok(ret)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Scraper::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let mut all_paths = Vec::new();

    for path in cli.paths {
        let mut paths = find_nested_dirs(&path, cli.depth)?;
        all_paths.append(&mut paths);
    }

    info!("Found {} directories", all_paths.len());

    let mut all_keys = Vec::new();
    for path in all_paths {
        let mut keys = find_solana_keypairs(&path)?;
        all_keys.append(&mut keys);
    }

    let mut all_keys_dedup: BTreeMap<Pubkey, BTreeSet<PathBuf>> = BTreeMap::new();

    for (dir, key) in all_keys.iter() {
        all_keys_dedup
            .entry(*key)
            .or_insert(BTreeSet::new())
            .insert(dir.clone());
    }

    info!(
        "Found {} distinct keypairs in {} locations",
        all_keys_dedup.len(),
        all_keys.len()
    );

    for (k, v) in all_keys_dedup.iter() {
        info!("{} in {} directories", k.to_string(), v.len());
    }

    let mut rpc_clients = Vec::new();
    for url in cli.rpc.iter() {
        let rpc = RpcClient::new(url.clone());

        match rpc.get_latest_blockhash().await {
            Ok(_bh) => rpc_clients.push(rpc),
            Err(e) => warn!(
                "Could not instantiate RPC client from {}, skipping. Error: {}",
                url,
                e.to_string()
            ),
        }
    }

    for pubkey in all_keys_dedup.keys() {
        let metadata_futs = rpc_clients.iter().map(|c| c.get_account(pubkey));

        let joined = futures::future::join_all(metadata_futs).await;

        for (rpc, metadata_result) in rpc_clients.iter().zip(joined.into_iter()) {
            match metadata_result {
                Ok(meta) => {
                    info!(
                        "{} on {}: {} SOL, owned by {}",
                        pubkey.to_string(),
                        rpc.url(),
                        solana_sdk::native_token::lamports_to_sol(meta.lamports),
                        meta.owner.to_string()
                    );
                }
                Err(e) => {
                    debug!(
                        "{} on {}: No data (error: {})",
                        pubkey.to_string(),
                        rpc.url(),
                        e
                    )
                }
            }
        }
    }

    Ok(())
}
