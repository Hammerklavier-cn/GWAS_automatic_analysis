use anyhow::{Result, anyhow};
use std::{
    any::TypeId,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use fm::VcfFile;

pub fn filter_snps<T: fm::FileManage + 'static>(
    source_file: T,
    snp_id_file: &Path,
    output_file_path: &Path,
) -> Result<VcfFile> {
    let vcftools_path = PathBuf::from_str(&std::env::var("VCFTOOLS_PATH").unwrap())?;

    let mut command = Command::new(vcftools_path.to_str().unwrap());
    // if source_file is vcf execute vcftools --vcf {source_file} --snps {snp_id_file} --recode vcf-iid --out {output_file}

    if source_file.type_id() == TypeId::of::<VcfFile>() {
        if source_file
            .get_path()
            .extension()
            .unwrap()
            .to_str()
            .unwrap()
            == "vcf"
        {
            command
                .arg("--vcf")
                .arg(source_file.get_path().to_str().unwrap())
                .arg("--snps")
                .arg(snp_id_file)
                .arg("--recode")
                .arg("--recode-INFO-all")
                .arg("--out")
                .arg(output_file_path);
        } else if source_file
            .get_path()
            .to_str()
            .unwrap()
            .ends_with(".vcf.gz")
        {
            command
                .arg("--gzvcf")
                .arg(source_file.get_path().to_str().unwrap())
                .arg("--snps")
                .arg(snp_id_file)
                .arg("--recode")
                .arg("--recode-INFO-all")
                .arg("--out")
                .arg(output_file_path);
        }
    } else {
        return Err(anyhow!("Not implemented yet."));
    };
    // TODO: if source_file is bed
    // execute plink --bfile {source_file} --extract {snp_id_file} --recode vcf-iid --out {output_file}

    let output = command
        .output()
        .map_err(|e| anyhow!("Failed to execute vcftools: {}", e))?;

    if !output.stderr.is_empty() {
        std::io::stderr().write_all(b"vcftools ERROR:")?;
        std::io::stderr().write_all(&output.stderr)?;
    }

    if true ^ output.status.success() {
        return Err(anyhow!("Failed to extract snps from given vcf file!"));
    }

    return Ok(
        VcfFile::builder(&output_file_path.with_extension("recode.vcf"))
            .will_be_deleted(true)
            .build()?,
    );
}
