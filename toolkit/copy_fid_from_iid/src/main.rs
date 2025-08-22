use std::{fs::File, path::PathBuf};

use clap::Parser;
use copy_fid_from_iid::standardize;
use polars::prelude::*;

mod errors;
mod parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
#[command(help_template = "\
{before-help}{name} Version {version}
Written by: {author}
{about}

{usage-heading} {usage}

{all-args}{after-help}
")]
struct Args {
    #[arg(short, long)]
    input_file: PathBuf,

    #[arg(short, long, help = "Path to the output file. Separator is tab.")]
    output_path: PathBuf,

    #[arg(
        long = "IID-index",
        default_value_t = 0,
        help = "Index of IID column in the input file. Counting starts from 0."
    )]
    iid_index: usize,

    #[arg(long, default_value_t = false)]
    has_header: bool,

    #[arg(
        long,
        default_value_t = '\t',
        help = "Separator character used in the input file. Default to tab."
    )]
    separator: char,

    #[arg(short = 'v', long, help = "Enable verbose mode")]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    if args.verbose {
        unsafe { std::env::set_var("RUST_LOG", "debug") }
        log::info!("Verbose mode enabled.");
    } else {
        unsafe { std::env::set_var("RUST_LOG", "info") }
        log::info!("Verbose mode disabled.");
    }

    env_logger::init();

    println!("Hello world!");

    log::info!("Input file: {}", args.input_file.display());
    log::info!("Export to: {}", args.output_path.display());
    log::info!("Separator: {}", args.separator);

    let lf = LazyCsvReader::new(PlPath::from_str(args.input_file.to_str().unwrap()))
        .with_separator(args.separator as u8)
        .with_has_header(args.has_header)
        .finish()
        .unwrap();

    let file_info = parser::scan_file(&args.input_file, args.separator, args.has_header).unwrap();

    log::info!("Got {} columns", file_info.columns);

    assert!(args.iid_index <= file_info.columns);

    log::info!("Generating result...");

    let res_lf = standardize(lf, args.iid_index as i64).unwrap();

    log::info!("Writing result to {}", args.output_path.display());

    let mut file = File::create(args.output_path).unwrap();

    CsvWriter::new(&mut file)
        .include_header(args.has_header)
        .with_separator(b'\t')
        .finish(&mut res_lf.collect().unwrap())
        .unwrap();

    log::info!("Done!");
}
