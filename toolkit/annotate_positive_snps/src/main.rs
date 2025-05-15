use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    /// Input file path
    #[arg(
        short,
        long,
        help = "Input file path. It should end with either .vcf(.gz) or .bed"
    )]
    input: String,
    /// Output file path
    #[arg(
        short,
        long,
        help = "Output file path. It should end with one of .csv, .tsv, .xlsx, .parquet."
    )]
    output: String,
}

fn main() {
    println!("Hello, world!");
}
