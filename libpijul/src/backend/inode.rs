use sanakirja::{Representable, Alignment};
use std;
use hex;
pub const INODE_SIZE: usize = 8;
/// A unique identifier for files or directories in the actual
/// file system, to map "files from the graph" to real files.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Inode([u8; INODE_SIZE]);
/// The `Inode` representing the root of the repository (on the
/// actual file system).
pub const ROOT_INODE: Inode = Inode([0; INODE_SIZE]);
impl std::ops::Deref for Inode {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}
impl std::ops::DerefMut for Inode {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl std::fmt::Debug for Inode {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Inode({})", hex::encode(&self.0))
    }
}
impl Inode {
    pub fn to_hex(&self) -> String {
        use hex::ToHex;
        let mut s = String::new();
        self.0.write_hex(&mut s).unwrap();
        s
    }

    pub fn is_root(&self) -> bool {
        *self == ROOT_INODE
    }

    /// Decode an inode from its hexadecimal representation.
    pub fn from_hex(hex: &str) -> Option<Inode> {
        let mut i = Inode([0; INODE_SIZE]);
        if super::from_hex(hex, &mut i) {
            Some(i)
        } else {
            None
        }
    }
}

impl Representable for Inode {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        std::mem::size_of::<Inode>() as u16
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        std::ptr::copy(self.0.as_ptr(), p, INODE_SIZE)
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        let mut i: Inode = std::mem::uninitialized();
        std::ptr::copy(p, i.0.as_mut_ptr(), INODE_SIZE);
        i
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
