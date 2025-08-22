use std::path::Path;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error<'a> {
    FileNotFound(&'a Path),
    FileIsDir(&'a Path),
    FileIsEmpty,
    FileIsNotReadable,
    ColumnsIndexOutOfBounds { index: usize, max: usize },
}
impl std::fmt::Display for Error<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileNotFound(path) => write!(f, "File not found: {}", path.display()),
            Error::FileIsDir(path) => write!(f, "{} is a directory, not a file", path.display()),
            Error::FileIsEmpty => write!(f, "File is empty"),
            Error::FileIsNotReadable => write!(f, "File is not readable"),
            Error::ColumnsIndexOutOfBounds { index, max } => write!(
                f,
                "Columns index out of bounds (index: {}, max: {})",
                index, max
            ),
        }
    }
}

impl std::error::Error for Error<'_> {}
