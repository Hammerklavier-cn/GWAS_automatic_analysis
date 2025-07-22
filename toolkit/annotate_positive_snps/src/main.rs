use anyhow::Result;
use clap::{Args, Parser};
use fm::{self, FileManage, VcfFile};
use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
};

#[derive(Debug, Parser)]
#[command(version, about)]
#[group(id = "input", required = true, multiple = false)]
struct Cli {
    /// Input file path
    #[arg(
        short,
        long,
        group = "input",
        help = "Input file path. It should end with either .vcf(.gz) or .bed",
        value_delimiter = ','
    )]
    vcfs: Option<Vec<PathBuf>>,

    #[arg(
        short = 'f',
        long,
        group = "input",
        help = "Folder containing input files"
    )]
    input_folder: Option<PathBuf>,

    #[arg(
        short = 's',
        long,
        help = "File containing SNP IDs, which you want to extract.",
        value_delimiter = ','
    )]
    snp_ids: PathBuf,

    #[clap(long, help = "Choose your dataset name. Possible values are `GRCh38.p14`, `GRCh37.87`, etc.", default_value_t = String::from("GRCh38.p14"))]
    dataset_name: String,

    #[command(flatten)]
    binary_paths: BinaryPath,

    /// Output folder path
    #[arg(short, long, help = "Prefix for output files.")]
    out: PathBuf,
}

#[derive(Args, Debug)]
struct BinaryPath {
    #[arg(long, help = "Path to vcftools binary")]
    vcftools: Option<PathBuf>,

    #[arg(long, help = "Path to vcf-concat binary")]
    vcf_concat: Option<PathBuf>,

    #[arg(long, help = "Path to snpEff.jar byte code")]
    snp_eff: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    println!("Args: {:?}", args);

    // std::process::exit(0);

    let vcf_tools = fm::Binary::Vcftools;
    let vcf_concat = fm::Binary::VcfConcat;
    let snp_eff = fm::Binary::SnpEff;

    // Set environment variables
    vcf_tools.set_env(args.binary_paths.vcftools.as_deref())?;
    vcf_concat.set_env(args.binary_paths.vcf_concat.as_deref())?;
    snp_eff.set_env(args.binary_paths.snp_eff.as_deref())?;

    let temp_path = args.out.parent().unwrap().join("temp");
    std::fs::create_dir_all(&temp_path)?;

    // Standardise input
    let mut vcf_files = Vec::new();
    let mut vcf_outputs = Vec::new();
    if let Some(vcf_paths) = args.vcfs {
        for path in vcf_paths.iter() {
            let vcf_file = VcfFile::builder(path).will_be_deleted(false).build()?;
            vcf_files.push(vcf_file);
        }
    }
    if let Some(folder) = args.input_folder {
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if ext == "vcf" || (ext == "gz" && path.to_str().unwrap().ends_with(".vcf.gz"))
                    {
                        let vcf_file = VcfFile::builder(&path).will_be_deleted(false).build()?;
                        vcf_files.push(vcf_file);
                    }
                }
            }
        }
    }

    // filter vcf
    println!("====Filtering vcf files...====");
    {
        let mut index = 0;
        let (tx, rx) = mpsc::channel::<Result<VcfFile>>();
        for vcf_file in vcf_files {
            index += 1;
            println!("Filtering vcf file {}", (*vcf_file).display());

            // multithreading optimisation
            let snp_id = Arc::new(args.snp_ids.clone());
            let temp_path = Arc::new(temp_path.clone());
            thread::spawn({
                let tx_clone = tx.clone();
                move || {
                    let vcf_res = filter_snp::filter_snps(
                        vcf_file,
                        &snp_id.clone(),
                        &temp_path.join(format!("filter_snp_{}", index)),
                    );
                    tx_clone.send(vcf_res).unwrap();
                }
            });
        }
        drop(tx);
        for res in rx {
            let vcf_file = res?;
            println!("Received {} from spawned thread", vcf_file.display());
            vcf_outputs.push(vcf_file);
        }
    }
    let vcf_files: Vec<VcfFile> = vcf_outputs;

    // Concatenate filtered vcfs
    println!("====Concatenating filtered vcf files...====");

    let vcf_file = concat_vcf::concat_vcf_files(vcf_files, &temp_path.join(format!("concated")))?;

    // annotation
    println!("====Anotating vcf file...====");
    let mut annotated_vcf = annotation::annotate_snps(vcf_file, &args.dataset_name, &args.out)?;

    annotated_vcf.set_will_be_deleted(false);

    println!("====Finished====");
    println!("Output vcf saved in {}", annotated_vcf.display());
    Ok(())
}
