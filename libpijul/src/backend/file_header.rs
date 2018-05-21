/// File metadata, essentially flags to indicate permissions and
/// nature of a file tracked by Pijul.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct FileMetadata(u16);
const DIR_BIT: u16 = 0x200;
use byteorder::ByteOrder;
impl FileMetadata {
    /// Read the file metadata from the file name encoded in the
    /// repository.
    pub fn from_contents(p: &[u8]) -> Self {
        debug_assert!(p.len() == 2);
        FileMetadata(BigEndian::read_u16(p))
    }

    /// Create a new file metadata with the given Unix permissions,
    /// and "is directory" bit.
    pub fn new(perm: usize, is_dir: bool) -> Self {
        let mut m = FileMetadata(0);
        m.set_permissions(perm as u16);
        if is_dir {
            m.set_dir()
        } else {
            m.unset_dir()
        }
        m
    }

    /// Permissions of this file (as in Unix).
    pub fn permissions(&self) -> u16 {
        u16::from_le(self.0) & 0x1ff
    }

    /// Set permissions of this file to the supplied parameters.
    pub fn set_permissions(&mut self, perm: u16) {
        let bits = u16::from_le(self.0);
        let perm = (bits & !0x1ff) | perm;
        self.0 = perm.to_le()
    }

    /// Tell whether this `FileMetadata` is a directory.
    pub fn is_dir(&self) -> bool {
        u16::from_le(self.0) & DIR_BIT != 0
    }

    /// Set this file metadata to be a directory.
    pub fn set_dir(&mut self) {
        let bits = u16::from_le(self.0);
        self.0 = (bits | DIR_BIT).to_le()
    }

    /// Set this file metadata to be a file.
    pub fn unset_dir(&mut self) {
        let bits = u16::from_le(self.0);
        self.0 = (bits & !DIR_BIT).to_le()
    }
}

use byteorder::{BigEndian, WriteBytesExt};

pub trait WriteMetadata: std::io::Write {
    fn write_metadata(&mut self, m: FileMetadata) -> std::io::Result<()> {
        self.write_u16::<BigEndian>(m.0)
    }
}
impl<W: std::io::Write> WriteMetadata for W {}


use sanakirja::{Representable, Alignment};
use std;
use super::key::*;
use super::patch_id::*;
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileStatus {
    Ok = 0,
    Moved = 1,
    Deleted = 2,
    Zombie = 3,
}

// Warning: FileMetadata is 16 bit-aligned, don't change the order.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct FileHeader {
    pub metadata: FileMetadata,
    pub status: FileStatus,
    pub key: Key<PatchId>,
}


#[test]
fn test_fileheader_alignment() {
    assert_eq!(std::mem::size_of::<FileHeader>(), 2 + 1 + 16)
}

impl Representable for FileHeader {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        std::mem::size_of::<FileHeader>() as u16
    }
    unsafe fn write_value(&self, p: *mut u8) {
        let meta = self.metadata.0;
        *p = (meta & 255) as u8;
        *(p.offset(1)) = (meta >> 8) as u8;
        *(p.offset(2)) = self.status as u8;
        self.key.write_value(p.offset(3))
    }
    unsafe fn read_value(p: *const u8) -> Self {
        let metadata = {
            let x0 = (*p) as u16;
            let x1 = (*(p.offset(1))) as u16;
            std::mem::transmute((x1 << 8) | x0)
        };
        let status = std::mem::transmute(*(p.offset(2)));
        let key: Key<PatchId> = Representable::read_value(p.offset(3));
        FileHeader { metadata, status, key }
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
