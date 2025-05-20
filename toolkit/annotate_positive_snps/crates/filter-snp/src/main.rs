use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use filter_snp::filter_snps;
use fm::{FileManage, VcfFile};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(
        short,
        long,
        help = "Path of input file, which should end with either .vcf(.gz) or .bed."
    )]
    input: PathBuf,

    #[arg(
        short,
        long = "snps",
        alias = "extract",
        help = "File containing positive SNPs. Each line should contain a SNP ID."
    )]
    snps: PathBuf,

    #[arg(short, long, help = "Path of vcftools executable")]
    vcftools_path: Option<PathBuf>,

    #[arg(
        short,
        long,
        help = "Path of output file, which contains no extension. A `.vcf` extension will be automatically generated."
    )]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Detect args: {:?}", args);

    // Detect vcftools
    match args.vcftools_path {
        Some(ref path) => {
            if path.exists() {
                unsafe {
                    std::env::set_var("VCFTOOLS_PATH", path.to_str().unwrap());
                }
            } else {
                return Err(format!("vcftools not found at {}", path.display()).into());
            }
        }
        None => {
            match std::env::var("VCFTOOLS_PATH") {
                Ok(path_string) => {
                    if !PathBuf::from_str(&path_string)?.exists() {
                        return Err(format!("vcftools not found at {}", path_string).into());
                    }
                }
                Err(_) => {
                    // Check if vcftools is in PATH
                    let vcftools_path = which::which(
                        if cfg!(target_os = "windows") {
                            "vcftools.exe"
                        } else {
                            "vcftools"
                        }
                    )
                        .map_err(|_| "vcftools not found in PATH. Add it to PATH, set VCFTOOLS_PATH environment variable or specify vcftools path with --vcftools-path")?;
                    unsafe {
                        std::env::set_var("VCFTOOLS_PATH", vcftools_path);
                    }
                }
            }
        }
    }

    // Concatenate input files
    let vcf_file = VcfFile::builder(&args.input)
        .will_be_deleted(false)
        .build()?;

    let mut result_vcf_file = filter_snps(vcf_file, &args.snps, &args.output)?;

    result_vcf_file.set_will_be_deleted(false);

    println!("Result has been saved in {}", (*result_vcf_file).display());

    Ok(())
}
