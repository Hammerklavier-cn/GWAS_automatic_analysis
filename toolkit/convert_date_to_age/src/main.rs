use std::path::PathBuf;

use anyhow::{Context, Error, Result};
use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    #[arg(
        short,
        long,
        help = "The file containing, and containing only (except f.eid column) the columns which you want to parse from date to age"
    )]
    input: String,

    #[arg(
        short,
        long,
        help = "The file containing the year when the candidates are born (and f.eid )"
    )]
    year: String,

    #[arg(
        short,
        long,
        help = "The file containing the month when the candidates are born (and f.eid column)"
    )]
    month: String,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    println!("{:?}", args);

    let input_path = PathBuf::try_from(&args.input)?.canonicalize()?;
    let year_path = PathBuf::try_from(&args.year)?.canonicalize()?;
    let month_path = PathBuf::try_from(&args.month)?.canonicalize()?;

    let parse_option = CsvParseOptions {
        separator: match &input_path.extension() {
            Some(s) => match s.to_str().unwrap() {
                "csv" => b',',
                _ => b'\t',
            },
            None => b'\t',
        },
        ..Default::default()
    };

    // read files
    let _input_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(0))
        .with_parse_options(parse_option.clone())
        .try_into_reader_with_file_path(Some(input_path.clone()))?
        .finish()?;
    let mut year_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(0))
        .with_parse_options(parse_option.clone())
        .try_into_reader_with_file_path(Some(year_path.clone()))?
        .finish()?;
    let mut month_df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(0))
        .with_parse_options(parse_option.clone())
        .try_into_reader_with_file_path(Some(month_path.clone()))?
        .finish()?;

    // parse the year in year_df
    {
        let year_columns = year_df
            .get_column_names_str()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        println!("{:?}", year_columns);

        for col_name in &year_columns[1..] {
            year_df = year_df
                .lazy()
                .with_column(
                    col(col_name)
                        .str()
                        .to_date(StrptimeOptions {
                            format: Some("%Y".into()),
                            ..Default::default()
                        })
                        .alias(format!("{}_date", col_name)),
                )
                .collect()?;
        }

        println!("year_df:\n{}", year_df);
    }

    {
        let month_columns = month_df
            .get_column_names_str()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        println!("{:?}", month_columns);

        for col_name in &month_columns[1..] {
            month_df = month_df
                .lazy()
                .with_column(
                    when(col(col_name).str().len_chars().eq(lit(1)))
                        .then(concat_str([lit("0"), col(col_name)], "", true))
                        .otherwise(col(col_name))
                        .alias(col_name),
                )
                .with_column(
                    col(col_name)
                        .str()
                        .to_date(StrptimeOptions {
                            format: Some("%m".into()),
                            strict: false,
                            ..Default::default()
                        })
                        .alias(format!("{}_date", col_name)),
                )
                .collect()?;
        }

        println!("month_df:\n{}", month_df);
    }

    {
        println!("input_df:\n{}", _input_df);
    }

    Ok(())
}
