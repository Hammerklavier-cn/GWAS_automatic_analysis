use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, help = "Path of annotated vcf(.gz) file.")]
    input: PathBuf,

    // #[arg(short, long, help = "")]
    #[arg(
        short,
        long,
        help = "Output file path. Support .csv, .xlsx and .parquet."
    )]
    output: PathBuf,
}

fn main() {
    let args = Args::parse();

    println!("Args: {:?}", args);
}
