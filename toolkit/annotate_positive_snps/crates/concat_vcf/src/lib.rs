use std::{ops::Deref, path::Path, process::Command};

use anyhow::{Result, anyhow};
use fm::{FileManage, VcfFile};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use fm::FileManage;
    use std::path::PathBuf;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn test_concat_vcf_files() -> Result<()> {
        let current_dir = std::env::current_dir()?;
        println!("Current working directory: {}", current_dir.display());
        let vcf_concat = fm::Binary::VcfConcat;

        vcf_concat.set_env::<&str>(None)?;

        let input_vcfs = vec![
            "../../run/MRC1_part1-filtered.recode.vcf",
            "../../run/MRC1_part2-filtered.recode.vcf",
            "../../run/MRC1_part3-filtered.recode.vcf",
        ]
        .into_iter()
        .map(|s| {
            VcfFile::builder(&PathBuf::from(s))
                .will_be_deleted(false)
                .build()
        })
        .collect::<Vec<_>>();

        let output_path = PathBuf::from("../../run/MRC1-concat-filtered.recode");

        let mut res_vcf = concat_vcf_files(input_vcfs, &output_path)?;
        res_vcf.set_will_be_deleted(false);

        Ok(())
    }
}

pub fn concat_vcf_files<T: AsRef<Path>>(
    input_vcfs: Vec<VcfFile>,
    output_path: T,
) -> Result<VcfFile> {
    let vcf_concat_path = std::env::var("VCFCONCAT_PATH").unwrap();

    // println!("Exec: {}", vcf_concat_path);
    let mut command = Command::new(vcf_concat_path);

    for vcf in input_vcfs {
        // println!("Add arg: {}", vcf.deref().to_str().unwrap());
        command.arg(vcf.deref().to_str().unwrap());
    }

    command.stdout(std::process::Stdio::piped());

    // println!("Execute!");

    let child = command
        .spawn()
        .map_err(|e| anyhow!("Failed to execute vcftools: {}", e))?;

    // println!("Right after execution");

    let command_output = child
        .stdout
        .ok_or_else(|| anyhow!("vcf-concat failed to generate any output."))?;

    // println!("Before writing");

    let mut output_vcf = VcfFile::builder(&output_path.as_ref().with_extension("vcf"))
        .will_be_deleted(true)
        .build();
    // println!("Right before writing");
    output_vcf.write_into(&mut std::io::BufReader::new(command_output))?;
    // println!("Right after writing");

    println!(
        "Finished writing vcf-concat result to {}",
        output_vcf.deref().display()
    );

    output_vcf.set_will_be_deleted(false);

    Ok(output_vcf)
}
