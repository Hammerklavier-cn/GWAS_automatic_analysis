use std::path::Path;

use fm::VcfFile;

pub fn annotate_snps(
    source_vcf: VcfFile,
    output_path: &Path,
) -> Result<VcfFile, Box<dyn std::error::Error>> {
    Err("`annotate_snps is not implemented yet".into())
}
