use anyhow::{Error, Result, anyhow};
use std::{ops::Deref, path::Path, process::Command};

use fm::VcfFile;

pub fn annotate_snps(
    source_vcf: VcfFile,
    dataset: &str,
    output_path: &Path,
) -> Result<VcfFile, Error> {
    match which::which("java") {
        Ok(java_path) => {
            println!("Java path: {}", java_path.display());
        }
        Err(e) => return Err(anyhow!("Cannot find java: {}", e)),
    };

    let snpeff_path = std::env::var("SNPEFF_PATH").unwrap_or_else(|_| {
        panic!("SNPEFF_PATH environment variable is not set");
    });

    let stats_path = Path::join(output_path.parent().unwrap(), "result_stats.csv");
    let html_path = Path::join(output_path.parent().unwrap(), "result_summary.html");
    println!("Save stats csv at {}", stats_path.display());

    // java -jar snpEff.jar ann GRCh38.p14 -csvStats stats.csv \
    // /home/jimmy/projects/GWAS_automatic_analysis/toolkit/annotate_positive_snps/run/MRC1_part1_filtered.recode.vcf \
    // > MRC1_part1-filtered-ann.vcf
    let mut command = Command::new("java");

    let params = vec![
        "-jar",
        &snpeff_path,
        "ann",
        dataset,
        "-csvStats",
        stats_path.to_str().unwrap(),
        "-htmlStats",
        html_path.to_str().unwrap(),
        source_vcf.deref().to_str().unwrap(),
    ];

    for param in &params {
        command.arg(param);
    }

    command.stdout(std::process::Stdio::piped());

    let child = command
        .spawn()
        .map_err(|e| anyhow!("Failed to execute snpeff: {}", e))?;

    let command_output = child
        .stdout
        .ok_or_else(|| anyhow!("snpeff failed to generate any output."))?;

    let output_vcf = VcfFile::builder(&output_path.with_extension("vcf"))
        .will_be_deleted(true)
        .set_check_existence(false)
        .build()?;
    output_vcf.write_into(&mut std::io::BufReader::new(command_output))?;

    println!("Finished writing result to {}", (*output_vcf).display());

    Ok(output_vcf)
}
