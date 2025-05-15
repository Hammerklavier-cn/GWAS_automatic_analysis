use std::{
    any::{Any, TypeId},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use fm::{FileType, VcfFile, VcfFileBuilder};

pub fn merge_vcf_files(
    vcf_files: Vec<VcfFile>,
    merged_vcf_path: &Path,
) -> Result<VcfFile, Box<dyn std::error::Error>> {
    let plink_path = PathBuf::from_str(&std::env::var("PLINK_PATH").unwrap())?;

    // execute `plink

    Err("Not implemented yet.".into())
}

pub fn filter_snps<T: fm::File + 'static>(
    source_file: T,
    snp_id_file: &Path,
    output_file_path: &Path,
) -> Result<VcfFile, Box<dyn std::error::Error>> {
    let plink_path = PathBuf::from_str(&std::env::var("PLINK_PATH").unwrap())?;

    let mut command = Command::new(plink_path.to_str().unwrap());
    // if source_file is vcf execute plink --vcf {source_file} --extract {snp_id_file} --recode vcf-iid --out {output_file}

    if source_file.type_id() == TypeId::of::<VcfFile>() {
        command
            .arg("--vcf")
            .arg(source_file.get_path().to_str().unwrap())
            .arg("--extract")
            .arg(snp_id_file)
            .arg("--recode")
            .arg("vcf-iid")
            .arg("--out")
            .arg(output_file_path);
    } else {
        return Err("Not implemented yet.".into());
    };
    // TODO: if source_file is bed
    // execute plink --bfile {source_file} --extract {snp_id_file} --recode vcf-iid --out {output_file}

    command
        .output()
        .map_err(|e| format!("Failed to execute plink: {}", e))?;

    return Ok(VcfFile::builder(&output_file_path)
        .will_be_deleted(true)
        .build());
}
