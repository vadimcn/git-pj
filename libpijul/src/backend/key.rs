use sanakirja::{Representable, Alignment};
use std;
use super::patch_id::*;
use Hash;
use bs58;

const LINE_ID_SIZE: usize = 8;
pub const KEY_SIZE: usize = PATCH_ID_SIZE + LINE_ID_SIZE;

/// The node at the root of the repository graph.
pub const ROOT_KEY: Key<PatchId> = Key {
    patch: ROOT_PATCH_ID,
    line: LineId([0; LINE_ID_SIZE]),
};

use hex::ToHex;
use std::fmt::Write;
impl ToHex for Key<PatchId> {
    fn write_hex<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        self.patch.write_hex(w)?;
        self.line.write_hex(w)
    }
    fn write_hex_upper<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        self.patch.write_hex(w)?;
        self.line.write_hex(w)
    }
}

impl Key<PatchId> {
    pub fn to_base58(&self) -> String {
        self.patch.to_base58() + &bs58::encode(&self.line.0).into_string()
    }
    pub fn to_hex(&self) -> String {
        let mut s = String::new();
        self.write_hex(&mut s).unwrap();
        s
    }
}

impl Key<Hash> {
    pub fn to_base58(&self) -> String {
        self.patch.to_base58() + &bs58::encode(&self.line.0).into_string()
    }
}

impl Key<PatchId> {

    /// Is this the root key? (the root key is all 0s).
    pub fn is_root(&self) -> bool {
        self == &ROOT_KEY
    }

    /// Decode this key from its hexadecimal representation.
    pub fn from_hex(hex: &str) -> Option<Self> {
        let mut s = [0; KEY_SIZE];
        if super::from_hex(hex, &mut s) {
            Some(unsafe { std::mem::transmute(s) })
        } else {
            None
        }
    }
}

// A LineId contains a counter encoded little-endian, so that it
// can both be deterministically put into a Sanakirja database,
// and passed to standard serializers.

/// An index for file chunks within a patch.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct LineId([u8; LINE_ID_SIZE]);

impl ToHex for LineId {
    fn write_hex<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        let mut h = String::new();
        self.0.write_hex(&mut h)?;
        w.write_str(h.trim_right_matches('0'))
    }
    fn write_hex_upper<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        let mut h = String::new();
        self.0.write_hex_upper(&mut h)?;
        w.write_str(h.trim_right_matches('0'))
    }
}

impl std::fmt::Debug for LineId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "LineId(0x{})", self.to_hex())
    }
}

impl LineId {
    /// Creates a new `LineId`, initialized to 0.
    pub fn new() -> LineId {
        LineId([0; LINE_ID_SIZE])
    }
    /// Is this line identifier all 0?
    pub fn is_root(&self) -> bool {
        self.0.iter().all(|x| *x == 0)
    }
    pub fn to_hex(&self) -> String {
        let mut s = String::new();
        self.write_hex(&mut s).unwrap();
        s
    }
}
use byteorder::{ByteOrder, LittleEndian};
impl std::ops::Add<usize> for LineId {
    type Output = LineId;
    fn add(self, x: usize) -> Self::Output {
        let a = LittleEndian::read_u64(&self.0);
        let mut b = LineId::new();
        LittleEndian::write_u64(&mut b.0, a + x as u64);
        b
    }
}
impl std::ops::AddAssign<usize> for LineId {
    fn add_assign(&mut self, x: usize) {
        *self = self.clone() + x
    }
}

/// A node in the repository graph, made of a patch internal
/// identifier, and a line identifier in that patch.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Key<H> {
    /// The patch that introduced this node.
    pub patch: H,
    /// The line identifier of the node in that patch. Here,
    /// "line" does not imply anything on the contents of the
    /// chunk.
    pub line: LineId,
}

#[test]
fn test_key_alignment() {
    assert_eq!(std::mem::size_of::<Key<PatchId>>(), 16)
}

impl<T> AsRef<LineId> for Key<T> {
    fn as_ref(&self) -> &LineId {
        &self.line
    }
}

impl<T: Clone> Key<Option<T>> {
    pub fn unwrap_patch(&self) -> Key<T> {
        Key {
            patch: self.patch.as_ref().unwrap().clone(),
            line: self.line.clone(),
        }
    }
}

impl Representable for Key<PatchId> {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        (PATCH_ID_SIZE + LINE_ID_SIZE) as u16
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        std::ptr::copy(self as *const Key<PatchId> as *const u8, p, KEY_SIZE)
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        let mut k = std::mem::uninitialized();
        std::ptr::copy(p, (&mut k) as *mut Key<PatchId> as *mut u8, 16);
        k
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.cmp(&x)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
