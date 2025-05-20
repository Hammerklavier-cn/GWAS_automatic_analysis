use anyhow::{Result, anyhow};
use std::{
    any::Any,
    ops::Deref,
    path::{Path, PathBuf},
};

pub enum FileType {
    Vcf,
    Bed,
}

#[allow(drop_bounds)]
pub trait FileManage: Deref + Drop + Any {
    fn get_will_be_deleted(&self) -> bool;
    fn set_will_be_deleted(&mut self, will_be_deleted: bool);
    fn type_of(&self) -> FileType;
    fn get_path(&self) -> &Path;
}

pub struct VcfFile {
    path: PathBuf,
    will_be_deleted: bool,
}
impl VcfFile {
    pub fn new(path: &Path) -> Self {
        VcfFile {
            path: path.to_path_buf(),
            will_be_deleted: false,
        }
    }
    pub fn builder(path: &Path) -> VcfFileBuilder {
        VcfFileBuilder::new(path)
    }

    pub fn write_into<R: std::io::Read>(&self, reader: &mut R) -> std::io::Result<()> {
        let mut file_writer = std::fs::File::create(&self.path)?;
        std::io::copy(reader, &mut file_writer)?;
        Ok(())
    }
}
impl Drop for VcfFile {
    fn drop(&mut self) {
        if self.get_will_be_deleted() {
            println!("Delete temp file {}", self.get_path().display());
            std::fs::remove_file(&self.path).unwrap();
        }
    }
}
impl Deref for VcfFile {
    type Target = PathBuf;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}
impl FileManage for VcfFile {
    fn get_will_be_deleted(&self) -> bool {
        self.will_be_deleted
    }
    fn type_of(&self) -> FileType {
        FileType::Vcf
    }
    fn get_path(&self) -> &Path {
        &self.path
    }
    fn set_will_be_deleted(&mut self, will_be_deleted: bool) {
        self.will_be_deleted = will_be_deleted;
    }
}

pub struct VcfFileBuilder {
    path: PathBuf,
    will_be_deleted: bool,
}
impl VcfFileBuilder {
    pub fn new(path: &Path) -> Self {
        VcfFileBuilder {
            path: path.to_path_buf(),
            will_be_deleted: false,
        }
    }

    pub fn will_be_deleted(mut self, will_be_deleted: bool) -> Self {
        self.will_be_deleted = will_be_deleted;
        self
    }

    pub fn build(self) -> VcfFile {
        VcfFile {
            path: self.path,
            will_be_deleted: self.will_be_deleted,
        }
    }
}

pub enum Binary {
    Vcftools,
    VcfConcat,
    Plink,
    SnpEff,
}
impl Binary {
    pub fn name(&self) -> String {
        match self {
            Binary::Vcftools => {
                if cfg!(windows) {
                    "vcftools.exe".to_string()
                } else {
                    "vcftools".to_string()
                }
            }
            Binary::VcfConcat => {
                if cfg!(windows) {
                    "vcf-concat.exe".to_string()
                } else {
                    "vcf-concat".to_string()
                }
            }
            Binary::Plink => {
                if cfg!(windows) {
                    "plink.exe".to_string()
                } else {
                    "plink".to_string()
                }
            }
            Binary::SnpEff => "snpEff.jar".to_string(),
        }
    }
    pub fn env_name(&self) -> String {
        match self {
            Binary::Vcftools => "VCFTOOLS_PATH".to_string(),
            Binary::VcfConcat => "VCFCONCAT_PATH".to_string(),
            Binary::Plink => "PLINK_PATH".to_string(),
            Binary::SnpEff => "SNPEFF_PATH".to_string(),
        }
    }

    #[cfg(feature = "set_env")]
    pub fn set_env<T: AsRef<Path>>(&self, value: Option<T>) -> Result<()> {
        match value {
            Some(path) => unsafe {
                std::env::set_var(self.env_name(), path.as_ref());
                Ok(())
            },
            None => match std::env::var(self.env_name()) {
                Ok(path) => match PathBuf::from(&path).exists() {
                    true => Ok(()),
                    false => Err(anyhow!(
                        "{} doesn't point to {}, which is not a valid file.",
                        self.env_name(),
                        path
                    )),
                },
                Err(_) => match which::which(&self.name()) {
                    Ok(path) => unsafe {
                        std::env::set_var(self.env_name(), path);
                        Ok(())
                    },
                    Err(_) => Err(anyhow!(
                        "{} can't be found in PATH. Neither is it designated by cli parameter or {}.",
                        self.name(),
                        self.env_name()
                    )),
                },
            },
        }
    }
}
