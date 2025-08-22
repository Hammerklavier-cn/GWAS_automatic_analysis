use super::errors;
use std::path::{Path, PathBuf};

#[derive(Default)]
#[allow(dead_code)]
pub struct FileInfo {
    pub path: PathBuf,
    pub name: String,
    pub separator: char,
    pub columns: usize,
    pub fields: Option<Vec<String>>,
    pub rows: usize,
}

pub fn scan_file<'a>(
    filepath: &'a Path,
    sep: char,
    has_header: bool,
) -> Result<FileInfo, Box<dyn std::error::Error + 'a>> {
    if filepath.is_dir() {
        return Err(Box::new(errors::Error::FileIsDir(filepath)));
    }

    // 使用 BufReader 逐行读取，避免将整个文件加载到内存
    let file = std::fs::File::open(filepath)?;
    let mut reader = std::io::BufReader::new(file);
    let mut first_line = String::new();

    std::io::BufRead::read_line(&mut reader, &mut first_line)?;

    if first_line.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "File is empty",
        )));
    }

    let first_line_fields: Vec<String> = first_line
        .trim_end() // 移除行尾的换行符
        .split(sep)
        .map(|s| s.trim().to_string())
        .collect();

    let columns = first_line_fields.len();

    let fields = match has_header {
        true => Some(first_line_fields),
        false => None,
    };

    let path_buf = PathBuf::from(filepath);

    let name = path_buf
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();

    let mut rows = match has_header {
        true => 0,
        false => 1,
    };

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = std::io::BufRead::read_line(&mut reader, &mut line)?;
        if bytes_read == 0 {
            break;
        }
        rows += 1;
    }

    Ok(FileInfo {
        path: path_buf,
        name,
        separator: sep,
        columns,
        fields,
        rows,
    })
}
