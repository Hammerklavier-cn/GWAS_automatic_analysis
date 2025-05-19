use std::{path::PathBuf, str::FromStr};

use annotation::annotate_snps;
use clap::Parser;
use fm::{FileManage, VcfFile};

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, help = "Input .vcf file for annotation")]
    input: PathBuf,

    #[clap(
        long,
        help = "Path to Snpeff. Note that we assume you have downloaded `GRCh38.p14` dataset."
    )]
    snpeff_path: Option<PathBuf>,

    #[clap(long, help = "Choose your dataset name. Possible values are `GRCh38.p14`, `GRCh37.87`, etc.", default_value_t = String::from("GRCh38.p14"))]
    dataset_name: String,

    #[clap(
        short,
        long,
        help = "Path to save the annotated VCF file. Note that you needn't specify the file extension. A `.vcf` ending will be added automatically."
    )]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Hello, world!");
    // Detect vcftools
    match args.snpeff_path {
        Some(ref path) => {
            if path.exists() {
                unsafe {
                    std::env::set_var("SNPEFF_PATH", path.to_str().unwrap());
                }
            } else {
                return Err(format!("snpeff not found at {}", path.display().to_string()).into());
            }
        }
        None => {
            match std::env::var("SNPEFF_PATH") {
                Ok(path_string) => {
                    if !PathBuf::from_str(&path_string)?.exists() {
                        return Err(format!("snpeff not found at {}", path_string).into());
                    }
                }
                Err(_) => {
                    // Check if snpeff is in PATH
                    let snpeff_path = which::which("snpeff.jar")
                        .map_err(|_| "snpeff not found in PATH. Add it to PATH, set SNPEFF_PATH environment variable or specify snpeff path with --snpeff-path")?;
                    unsafe {
                        std::env::set_var("SNPEFF_PATH", snpeff_path);
                    }
                }
            }
        }
    }

    let source_vcf = VcfFile::builder(&args.input).will_be_deleted(false).build();

    let mut output_vcf = annotate_snps(source_vcf, &args.dataset_name, &args.output)?;

    output_vcf.set_will_be_deleted(false);

    Ok(())
}
