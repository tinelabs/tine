use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Schema};
use dashmap::DashMap;
use tracing::debug;

use tine_core::{ArtifactKey, ArtifactStore, NodeId, TineError, TineResult};

// ---------------------------------------------------------------------------
// MappedArtifact — a memory-mapped Arrow IPC file
// ---------------------------------------------------------------------------

/// A memory-mapped artifact with its schema and reference count.
pub struct MappedArtifact {
    /// The memory-mapped region.
    pub mmap: memmap2::Mmap,
    /// The Arrow schema extracted from the IPC file.
    pub schema: Schema,
    /// Path to the artifact file on disk.
    pub path: PathBuf,
    /// Reference count for GC.
    pub refcount: AtomicUsize,
}

// ---------------------------------------------------------------------------
// DataCatalog — zero-copy data access layer
// ---------------------------------------------------------------------------

/// Provides schema-validated, zero-copy access to Arrow IPC artifacts.
pub struct DataCatalog {
    /// In-memory cache of mapped artifacts.
    mmaps: DashMap<ArtifactKey, Arc<MappedArtifact>>,
    /// The underlying artifact store.
    store: Arc<dyn ArtifactStore>,
    /// Local artifact directory for mmap.
    artifact_dir: PathBuf,
}

impl DataCatalog {
    pub fn new(store: Arc<dyn ArtifactStore>, artifact_dir: PathBuf) -> Self {
        Self {
            mmaps: DashMap::new(),
            store,
            artifact_dir,
        }
    }

    /// Load an artifact into memory-mapped cache, extracting its schema.
    pub async fn load(&self, key: &ArtifactKey) -> TineResult<Arc<MappedArtifact>> {
        // Check if already loaded
        if let Some(existing) = self.mmaps.get(key) {
            existing.refcount.fetch_add(1, Ordering::Relaxed);
            return Ok(existing.clone());
        }

        // Ensure artifact is on local disk
        let artifact_path = self.artifact_dir.join(key.as_str());
        if !artifact_path.exists() {
            // Download from store
            let data = self.store.get(key).await?;
            tokio::fs::create_dir_all(artifact_path.parent().unwrap()).await?;
            tokio::fs::write(&artifact_path, &data).await?;
        }

        // Memory-map the file
        let file = std::fs::File::open(&artifact_path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        // Extract schema from IPC
        let schema = Self::extract_schema(&mmap)?;

        debug!(
            key = %key,
            columns = schema.fields().len(),
            "loaded artifact into catalog"
        );

        let mapped = Arc::new(MappedArtifact {
            mmap,
            schema,
            path: artifact_path,
            refcount: AtomicUsize::new(1),
        });

        self.mmaps.insert(key.clone(), mapped.clone());
        Ok(mapped)
    }

    /// Extract the Arrow schema from IPC file bytes.
    fn extract_schema(data: &[u8]) -> TineResult<Schema> {
        let cursor = std::io::Cursor::new(data);
        let reader = FileReader::try_new(cursor, None)
            .map_err(|e| TineError::SchemaValidation(format!("failed to read Arrow IPC: {}", e)))?;
        Ok(reader.schema().as_ref().clone())
    }

    /// Validate both column names AND data types against expectations.
    pub fn validate_schema(
        &self,
        key: &ArtifactKey,
        expected: &[(String, DataType)],
    ) -> TineResult<()> {
        let artifact = self
            .mmaps
            .get(key)
            .ok_or_else(|| TineError::ArtifactNotFound(key.clone()))?;

        for (col_name, expected_type) in expected {
            match artifact.schema.field_with_name(col_name) {
                Ok(field) => {
                    if !types_compatible(field.data_type(), expected_type) {
                        return Err(TineError::TypeMismatch {
                            node: NodeId::new(""),
                            column: col_name.clone(),
                            expected: format!("{:?}", expected_type),
                            actual: format!("{:?}", field.data_type()),
                        });
                    }
                }
                Err(_) => {
                    let available: Vec<String> = artifact
                        .schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                    return Err(TineError::MissingColumn {
                        node: NodeId::new(""),
                        input: String::new(),
                        missing: col_name.clone(),
                        available,
                    });
                }
            }
        }
        Ok(())
    }

    /// Get the schema of a loaded artifact.
    pub fn schema(&self, key: &ArtifactKey) -> Option<Schema> {
        self.mmaps.get(key).map(|a| a.schema.clone())
    }

    /// Get the raw bytes of a loaded artifact (zero-copy via mmap).
    pub fn bytes(&self, key: &ArtifactKey) -> Option<bytes::Bytes> {
        self.mmaps
            .get(key)
            .map(|a| bytes::Bytes::copy_from_slice(&a.mmap))
    }

    /// Release a reference to an artifact.
    pub fn release(&self, key: &ArtifactKey) {
        if let Some(artifact) = self.mmaps.get(key) {
            let prev = artifact.refcount.fetch_sub(1, Ordering::Relaxed);
            if prev <= 1 {
                drop(artifact);
                self.mmaps.remove(key);
            }
        }
    }

    /// Get number of loaded artifacts.
    pub fn loaded_count(&self) -> usize {
        self.mmaps.len()
    }

    /// Store artifact bytes and add to catalog.
    pub async fn store(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
        // Store to backend
        let hash = self.store.put(key, data).await?;

        // Write locally for mmap
        let artifact_path = self.artifact_dir.join(key.as_str());
        tokio::fs::create_dir_all(artifact_path.parent().unwrap()).await?;
        tokio::fs::write(&artifact_path, data).await?;

        Ok(hash)
    }

    /// Register an already-existing artifact file into the catalog via memory-map.
    ///
    /// This is used when we have a file path (e.g., from `_pf_save_artifact`) and
    /// want to mmap it + extract the Arrow schema without loading into Rust heap.
    pub async fn register(&self, key: ArtifactKey, path: std::path::PathBuf) -> TineResult<()> {
        if self.mmaps.contains_key(&key) {
            return Ok(()); // already registered
        }

        let file =
            std::fs::File::open(&path).map_err(|_e| TineError::ArtifactNotFound(key.clone()))?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        // Gracefully handle non-Arrow files (e.g. cloudpickle .pkl)
        let schema = Self::extract_schema(&mmap).unwrap_or_else(|_| Schema::empty());

        debug!(
            key = %key,
            path = %path.display(),
            columns = schema.fields().len(),
            "registered artifact in catalog"
        );

        let mapped = Arc::new(MappedArtifact {
            mmap,
            schema,
            path,
            refcount: AtomicUsize::new(1),
        });

        self.mmaps.insert(key, mapped);
        Ok(())
    }

    /// Get the local file path for a loaded artifact (for injection into kernels).
    /// Falls back to checking the artifact directory on disk if not in memory.
    pub fn get_path(&self, key: &ArtifactKey) -> Option<PathBuf> {
        // Check in-memory cache first
        if let Some(a) = self.mmaps.get(key) {
            return Some(a.path.clone());
        }
        // Fallback: check if the artifact file exists on disk
        let path = self.artifact_dir.join(key.as_str());
        if path.exists() {
            return Some(path);
        }
        None
    }

    /// Get the local artifact directory.
    pub fn artifact_dir(&self) -> &std::path::Path {
        &self.artifact_dir
    }
}

/// Check if types are compatible (allow safe promotions).
fn types_compatible(actual: &DataType, expected: &DataType) -> bool {
    if actual == expected {
        return true;
    }
    matches!(
        (actual, expected),
        (DataType::Int8, DataType::Int16)
            | (DataType::Int8, DataType::Int32)
            | (DataType::Int8, DataType::Int64)
            | (DataType::Int16, DataType::Int32)
            | (DataType::Int16, DataType::Int64)
            | (DataType::Int32, DataType::Int64)
            | (DataType::Float32, DataType::Float64)
            | (DataType::Utf8, DataType::LargeUtf8)
    )
}
