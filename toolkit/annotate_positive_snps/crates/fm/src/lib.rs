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
pub trait File: Deref + Drop + Any {
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
impl File for VcfFile {
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
