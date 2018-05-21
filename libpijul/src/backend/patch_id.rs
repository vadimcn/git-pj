use sanakirja::{Representable, Alignment};
use std;
use bs58;

// Patch Identifiers.
pub const PATCH_ID_SIZE: usize = 8;
pub const ROOT_PATCH_ID: PatchId = PatchId([0; PATCH_ID_SIZE]);

/// An internal patch identifier, less random than external patch
/// identifiers, but more stable in time, and much smaller.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct PatchId([u8; PATCH_ID_SIZE]);

impl std::fmt::Debug for PatchId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "PatchId({})", self.to_base58())
    }
}

use hex::ToHex;
use std::fmt::Write;
impl ToHex for PatchId {
    fn write_hex<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        self.0.write_hex(w)
    }
    fn write_hex_upper<W:Write>(&self, w: &mut W) -> std::fmt::Result {
        self.0.write_hex_upper(w)
    }
}
impl PatchId {
    /// New patch id (initialised to 0).
    pub fn new() -> Self {
        PatchId([0; PATCH_ID_SIZE])
    }
    /// Encode this patch id in base58.
    pub fn to_base58(&self) -> String {
        bs58::encode(&self.0).into_string()
    }
    /// Decode this patch id from its base58 encoding.
    pub fn from_base58(s: &str) -> Option<Self> {
        let mut p = PatchId::new();
        if bs58::decode(s).into(&mut p.0).is_ok() {
            Some(p)
        } else {
            None
        }
    }
    pub fn is_root(&self) -> bool {
        *self == ROOT_PATCH_ID
    }
}

impl std::ops::Deref for PatchId {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for PatchId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Representable for PatchId {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        8
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        std::ptr::copy(self.0.as_ptr(), p, PATCH_ID_SIZE)
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        let mut patch_id: PatchId = std::mem::uninitialized();
        std::ptr::copy(p, patch_id.0.as_mut_ptr(), 8);
        patch_id
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        self.0.cmp(&x.0)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
