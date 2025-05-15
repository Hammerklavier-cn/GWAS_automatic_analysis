use std::{
    any::TypeId,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use fm::VcfFile;

pub fn filter_snps<T: fm::File + 'static>(
    source_file: T,
    snp_id_file: &Path,
    output_file_path: &Path,
) -> Result<VcfFile, Box<dyn std::error::Error>> {
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
                .arg(output_file_path.with_extension(""));
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
                .arg(output_file_path.with_extension("").with_extension(""));
        }
    } else {
        return Err("Not implemented yet.".into());
    };
    // TODO: if source_file is bed
    // execute plink --bfile {source_file} --extract {snp_id_file} --recode vcf-iid --out {output_file}

    let output = command
        .output()
        .map_err(|e| format!("Failed to execute plink: {}", e))?;

    std::io::stderr().write_all(&output.stderr)?;

    if true ^ output.status.success() {
        return Err("Failed to extract snps from given vcf file!".into());
    }

    return Ok(VcfFile::builder(&output_file_path)
        .will_be_deleted(true)
        .build());
}
