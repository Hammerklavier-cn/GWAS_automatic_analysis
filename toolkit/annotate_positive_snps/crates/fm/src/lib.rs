use std::{ops::Deref, path::PathBuf};

pub trait File: Deref + Drop {
    fn get_will_be_deleted(&self) -> bool;
}

pub struct VcfFile {
    path: PathBuf,
    will_be_deleted: bool,
}

impl VcfFile {
    pub fn new(path: PathBuf) -> Self {
        VcfFile {
            path,
            will_be_deleted: false,
        }
    }
    pub fn builder(path: PathBuf) -> VcfFileBuilder {
        VcfFileBuilder::new(path)
    }
}

impl Drop for VcfFile {
    fn drop(&mut self) {
        if self.get_will_be_deleted() {
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
}

pub struct VcfFileBuilder {
    path: PathBuf,
    will_be_deleted: bool,
}

impl VcfFileBuilder {
    pub fn new(path: PathBuf) -> Self {
        VcfFileBuilder {
            path,
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
