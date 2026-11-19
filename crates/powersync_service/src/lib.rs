use log::info;

pub mod sync_plan;

// Just a stub for now.
pub struct PowerSyncService {}

impl PowerSyncService {
    pub fn init(embedder_name: &str) -> Self {
        info!("Initializing Rust module for {embedder_name}");

        Self {}
    }
}
