use sanakirja::{Representable, Alignment};
use std;
use super::inode::*;
use super::small_string::*;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
#[repr(packed)]
pub struct OwnedFileId {
    pub parent_inode: Inode,
    pub basename: SmallString,
}

impl OwnedFileId {
    pub fn as_file_id(&self) -> FileId {
        FileId {
            parent_inode: self.parent_inode,
            basename: self.basename.as_small_str(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FileId<'a> {
    pub parent_inode: Inode,
    pub basename: SmallStr<'a>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnsafeFileId {
    parent_inode: Inode,
    basename: UnsafeSmallStr,
}

impl<'a> FileId<'a> {
    pub fn to_owned(&self) -> OwnedFileId {
        OwnedFileId {
            parent_inode: self.parent_inode.clone(),
            basename: self.basename.to_owned(),
        }
    }
    pub fn to_unsafe(&self) -> UnsafeFileId {
        UnsafeFileId {
            parent_inode: self.parent_inode,
            basename: self.basename.to_unsafe(),
        }
    }
    pub unsafe fn from_unsafe(p: UnsafeFileId) -> FileId<'a> {
        FileId {
            parent_inode: p.parent_inode,
            basename: SmallStr::from_unsafe(p.basename),
        }
    }
}

impl Representable for UnsafeFileId {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        INODE_SIZE as u16 + self.basename.onpage_size()
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        self.parent_inode.write_value(p);
        self.basename.write_value(p.offset(INODE_SIZE as isize));
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        UnsafeFileId {
            parent_inode: Inode::read_value(p),
            basename: UnsafeSmallStr::read_value(p.offset(INODE_SIZE as isize)),
        }
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        trace!("cmp_value file_id");
        let a: FileId = FileId::from_unsafe(*self);
        let b: FileId = FileId::from_unsafe(x);
        a.cmp(&b)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
