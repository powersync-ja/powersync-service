fn hash_json<T: Serialize>(value: &T) -> Result<u32, CompilerError> {
    let json = serde_json::to_string(value)?;
    let mut hash: u32 = 2166136261;
    for byte in json.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16777619);
    }

    Ok(hash)
}

fn canonicalize_sources_and_buckets(
    data_sources: &mut Vec<SerializedDataSource>,
    buckets: &mut Vec<SerializedBucketDataSource>,
    streams: &mut Vec<SerializedStream>,
) -> Result<(), CompilerError> {
    let mut source_signature_map = HashMap::<String, usize>::new();
    let mut source_remap = vec![0; data_sources.len()];
    let mut canonical_sources = Vec::<SerializedDataSource>::new();

    for (old_index, source) in data_sources.iter().cloned().enumerate() {
        let signature = serde_json::to_string(&(
            source.table.clone(),
            source.output_table_name.clone(),
            source.columns.clone(),
            source.filters.clone(),
            source.partition_by.clone(),
        ))?;

        if let Some(index) = source_signature_map.get(&signature) {
            source_remap[old_index] = *index;
        } else {
            let mut source = source;
            source.hash = hash_json(&source)?;
            let index = canonical_sources.len();
            source_signature_map.insert(signature, index);
            source_remap[old_index] = index;
            canonical_sources.push(source);
        }
    }
    *data_sources = canonical_sources;

    let mut bucket_signature_map = HashMap::<String, usize>::new();
    let mut bucket_remap = vec![0; buckets.len()];
    let mut canonical_buckets = Vec::<SerializedBucketDataSource>::new();

    for (old_index, bucket) in buckets.iter().cloned().enumerate() {
        let mapped_sources = bucket
            .sources
            .into_iter()
            .map(|index| source_remap[index])
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut sources = Vec::new();
        for source in mapped_sources {
            if seen.insert(source) {
                sources.push(source);
            }
        }

        let mut signature_sources = sources.clone();
        signature_sources.sort_unstable();
        let signature = serde_json::to_string(&signature_sources)?;
        if let Some(index) = bucket_signature_map.get(&signature) {
            bucket_remap[old_index] = *index;
        } else {
            let index = canonical_buckets.len();
            let mut canonical_bucket = SerializedBucketDataSource {
                hash: 0,
                unique_name: bucket.unique_name,
                sources,
            };
            canonical_bucket.hash = hash_json(&canonical_bucket)?;
            bucket_signature_map.insert(signature, index);
            bucket_remap[old_index] = index;
            canonical_buckets.push(canonical_bucket);
        }
    }
    *buckets = canonical_buckets;

    for stream in streams {
        for querier in &mut stream.queriers {
            querier.bucket = bucket_remap[querier.bucket];
        }
    }

    Ok(())
}
