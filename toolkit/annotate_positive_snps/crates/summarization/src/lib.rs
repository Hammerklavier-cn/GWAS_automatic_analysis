use anyhow::{Context, Result, anyhow};
use fm::{FileManage, VcfFile};
use std::{
    fs::File,
    io::{self, BufRead, Write},
    path::Path,
};
use vcf::VcfRecord;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

/// Read and parse a vcf file, extract information in the `ANN=` section, recode and save in a csv/tsv file.
pub fn parse_ann_vcf_result<T: AsRef<Path>>(vcf_file: VcfFile, save_path: T) -> Result<()> {
    let file_path = vcf_file.get_path();
    // Open vcf file
    let vcf_file = File::open(&file_path)
        .with_context(move || anyhow!("Failed to open {}", file_path.display()))?;
    let reader = io::BufReader::new(vcf_file);

    // create save file
    let result_file = File::create(&save_path).with_context(move || {
        anyhow!("Failed to create file at {}", save_path.as_ref().display())
    })?;
    let mut writer = io::BufWriter::new(result_file);

    let mut ann_header: Option<Vec<String>> = None;

    let mut line_index: u64 = 0;
    for line in reader.lines().map(|l| l.unwrap()) {
        line_index = line_index + 1;
        // TODO: Implement parsing logic here
        let parsed = vcf::parse_vcf_line(&line)?;

        match parsed {
            VcfRecord::Header(headers) => {
                ann_header = Some(headers.clone());
                writer.write(&format!("{}\n", headers.join("\t")).into_bytes())?;
            }
            VcfRecord::Record(records) => match &ann_header {
                Some(header) => {
                    if header.len() != records.len() {
                        return Err(anyhow!(
                            "ANN INFO length doesn't match header length at line {}. Expect: {}; Got: {}",
                            line_index,
                            header.len(),
                            records.len()
                        ));
                    } else {
                        writer.write(&format!("{}\n", records.join("\t")).into_bytes())?;
                    }
                }
                None => {
                    return Err(anyhow!(
                        "Got ANN INFO at line {}, but header has not been detected!",
                        line_index
                    ));
                }
            },
            VcfRecord::None => continue,
        }
    }

    return Err(anyhow!("Not implemented yet!"));
}

pub(crate) mod vcf {
    use anyhow::{Context, Result, anyhow};
    use regex::Regex;

    pub(super) enum VcfRecord {
        Header(Vec<String>),
        Record(Vec<String>),
        None,
    }

    pub(super) fn parse_vcf_line<T: AsRef<str>>(line: T) -> Result<VcfRecord> {
        let re_header = Regex::new(r#""Functional annotations: '([^']*)'"#)?;

        match line.as_ref() {
            line if line.starts_with("##INFO") => {
                if let Some(caps) = re_header.captures(line) {
                    if let Some(content) = caps.get(1) {
                        // 分割字段并去除前后空格
                        let fields: Vec<String> = content
                            .as_str()
                            .split('|')
                            .map(|s| s.trim().to_string())
                            .collect();
                        return Ok(VcfRecord::Header(fields));
                    } else {
                        return Err(anyhow!("Regex pattern failed to get ANN columns!"));
                    }
                } else {
                    return Ok(VcfRecord::None);
                }
            }
            line if !line.starts_with("#") => {
                let fields: Vec<String> = line
                    .split('\t')
                    .nth(7)
                    .with_context(|| anyhow!("Failed to get the 8th object of {}", line))?
                    .split('|')
                    .map(|s| s.trim().to_string())
                    .collect();
                return Ok(VcfRecord::Record(fields));
            }
            _ => return Ok(VcfRecord::None),
        };

        // return Ok(VcfRecord::Header(vec![]));
        // match line.as_ref().split('\t').collect::<Vec<&str>>().as_slice() {
        //     ["##fileformat", ..] => {
        //         VcfRecord::Header(line.as_ref().split('\t').map(|s| s.to_string()).collect())
        //     }
        //     [
        //         chrom,
        //         pos,
        //         id,
        //         ref_allele,
        //         alt_allele,
        //         qual,
        //         filter,
        //         info,
        //         format,
        //         ..,
        //     ] => VcfRecord::Record(vec![
        //         chrom.to_string(),
        //         pos.to_string(),
        //         id.to_string(),
        //         ref_allele.to_string(),
        //         alt_allele.to_string(),
        //         qual.to_string(),
        //         filter.to_string(),
        //         info.to_string(),
        //         format.to_string(),
        //     ]),
        //     _ => panic!("Invalid VCF line"),
        // }
    }
}
