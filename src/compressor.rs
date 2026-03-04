use vortex::{
    array::{Array, ArrayRef, Canonical, IntoArray},
    compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder, IntCode},
    dtype::DType,
    encodings::zstd::ZstdArray,
    error::VortexResult,
    layout::layouts::compressed::CompressorPlugin,
};

#[derive(Clone)]
pub struct Utf8Compressor {
    /// The underlying BtrBlocks compressor for general compression
    btr_compressor: BtrBlocksCompressor,
    /// Zstd compression level (default: 3)
    zstd_level: i32,
    /// Number of values per Zstd compression frame (default: 8192)
    values_per_page: usize,
}

impl Utf8Compressor {
    /// Create a new smart compressor with default settings.
    pub fn new() -> Self {
        Self {
            btr_compressor: BtrBlocksCompressorBuilder::default()
                .exclude_int([IntCode::Dict])
                .build(),
            zstd_level: 3,
            values_per_page: 8192,
        }
    }

    /// Compress a chunk of data.
    pub fn compress(&self, chunk: &dyn Array) -> VortexResult<ArrayRef> {
        // Check if this is a UTF8 or Binary field
        if matches!(chunk.dtype(), DType::Utf8(_) | DType::Binary(_)) {
            self.compress_utf8_or_binary(chunk)
        } else {
            // For non-UTF8 types, use BtrBlocks directly
            self.btr_compressor.compress(chunk)
        }
    }

    fn compress_utf8_or_binary(&self, chunk: &dyn Array) -> VortexResult<ArrayRef> {
        let canonical = chunk.to_canonical()?;
        let compressed = match &canonical {
            Canonical::VarBinView(vbv) => {
                let zstd_array =
                    ZstdArray::from_var_bin_view(vbv, self.zstd_level, self.values_per_page)?;
                zstd_array.into_array()
            }
            _ => {
                // Unexpected canonical form, return BtrBlocks result
                self.btr_compressor.compress(chunk)?
            }
        };

        Ok(compressed)
    }
}

impl Default for Utf8Compressor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressorPlugin for Utf8Compressor {
    fn compress_chunk(&self, chunk: &dyn Array) -> VortexResult<ArrayRef> {
        self.compress(chunk)
    }
}

#[cfg(test)]
mod tests {
    use vortex::{
        array::{IntoArray, arrays::VarBinViewArray},
        dtype::{DType, Nullability},
    };

    use super::*;

    #[test]
    fn test_compresses_utf8_strings() {
        let compressor = Utf8Compressor::new();

        let strings = vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("cherry"),
            Some("banana"),
            Some("apple"),
            Some("cherry"),
            Some("banana"),
            Some("apple"),
            Some("apple"),
            Some("banana"),
            Some("cherry"),
        ];
        let array =
            VarBinViewArray::from_iter(strings, DType::Utf8(Nullability::NonNullable)).into_array();

        let compressed = compressor.compress(&array).unwrap();
        assert_eq!(compressed.len(), array.len());
        assert!(compressed.nbytes() > 0);
    }

    #[test]
    fn test_high_cardinality_strings() {
        let compressor = Utf8Compressor::new();

        let strings: Vec<Option<String>> = (0..100)
            .map(|i| Some(format!("unique_string_{:06}", i)))
            .collect();
        let array =
            VarBinViewArray::from_iter(strings, DType::Utf8(Nullability::NonNullable)).into_array();

        let compressed = compressor.compress(&array).unwrap();
        assert_eq!(compressed.len(), 100);
        assert!(compressed.nbytes() > 0);
    }

    #[test]
    fn test_non_utf8_uses_btrblocks() {
        use vortex::array::arrays::PrimitiveArray;

        let compressor = Utf8Compressor::new();
        let array: PrimitiveArray = vec![1i32, 2, 3, 4, 5].into_iter().collect();

        let compressed = compressor.compress(array.as_ref()).unwrap();
        assert_eq!(compressed.len(), 5);
    }

    #[test]
    fn test_empty_string_array() {
        let compressor = Utf8Compressor::new();

        let strings: Vec<Option<&str>> = vec![];
        let array =
            VarBinViewArray::from_iter(strings, DType::Utf8(Nullability::NonNullable)).into_array();

        let compressed = compressor.compress(&array).unwrap();
        assert_eq!(compressed.len(), 0);
    }
}
