use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use fm::VcfFile;
use summarization::parse_ann_vcf_result;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, help = "Path of annotated vcf file.")]
    vcf: PathBuf,

    #[arg(
        short,
        long,
        help = "Output file path. Support .csv, .xlsx and .parquet."
    )]
    output: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Args: {:?}", args);

    let vcf_file = VcfFile::builder(&args.vcf).will_be_deleted(false).build()?;

    parse_ann_vcf_result(vcf_file, &args.output)?;

    Ok(())
}
