use std::{path::PathBuf, str::FromStr};

use clap::Parser;
use fm::{VcfFile, VcfFileBuilder};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(
        short,
        long,
        value_delimiter = ',',
        help = "Paths of input files, each of which should end with either .vcf(.gz) or .bed, all having the same extension."
    )]
    input: Vec<PathBuf>,

    #[arg(
        short,
        long,
        help = "File containing positive SNPs. Each line should contain a SNP ID."
    )]
    keep: PathBuf,

    #[arg(short, long, help = "Path of plink executable")]
    plink_path: Option<PathBuf>,

    #[arg(
        short,
        long,
        help = "Path of output file, which should end with either .vcf(.gz) or .bed."
    )]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Detect args: {:?}", args);

    // Detect plink
    match args.plink_path {
        Some(ref path) => {
            if path.exists() {
                unsafe {
                    std::env::set_var("PLINK_PATH", path.to_str().unwrap());
                }
            } else {
                return Err(format!("plink not found at {}", path.display()).into());
            }
        }
        None => {
            match std::env::var("PLINK_PATH") {
                Ok(path_string) => {
                    if !PathBuf::from_str(&path_string)?.exists() {
                        return Err(format!("plink not found at {}", path_string).into());
                    }
                }
                Err(_) => {
                    // Check if plink is in PATH
                    let plink_path = which::which(
                        if cfg!(target_os = "windows") {
                            "plink.exe"
                        } else {
                            "plink"                        }
                    )
                        .map_err(|_| "plink not found in PATH. Add it to PATH, set PLINK_PATH environment variable or specify plink path with --plink-path")?;
                    unsafe {
                        std::env::set_var("PLINK_PATH", plink_path);
                    }
                }
            }
        }
    }

    // Concatenate input files
    let mut vcf_files = Vec::new();
    for input in args.input {
        let vcf_file = VcfFile::builder(&input).will_be_deleted(false).build();
        vcf_files.push(vcf_file);
    }

    Err("Not finished".into())
}
