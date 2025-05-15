use clap::Parser;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(
        short,
        long,
        value_delimiter = ',',
        help = "Paths of input files, each of which should end with either .vcf(.gz) or .bed, all having the same extension."
    )]
    input: Vec<String>,

    #[arg(
        short,
        long,
        help = "File containing positive SNPs. Each line should contain a SNP ID."
    )]
    keep: String,

    #[arg(
        short,
        long,
        help = "Path of output file, which should end with either .vcf(.gz) or .bed."
    )]
    output: String,
}

fn main() {
    let args = Args::parse();
    println!("Detect args: {:?}", args);
}
