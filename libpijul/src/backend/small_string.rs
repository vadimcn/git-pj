use sanakirja::{Representable, Alignment};
use std;
pub const MAX_LENGTH: usize = 255;

/// A string of length at most 255, with a more compact on-disk
/// encoding.
#[repr(packed)]
pub struct SmallString {
    pub len: u8,
    pub str: [u8; MAX_LENGTH],
}

/// A borrowed version of `SmallStr`.
#[derive(Clone, Copy)]
pub struct SmallStr<'a>(*const u8, std::marker::PhantomData<&'a ()>);

impl Clone for SmallString {
    fn clone(&self) -> Self {
        Self::from_str(self.as_str())
    }
}

impl std::fmt::Debug for SmallString {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_small_str().fmt(fmt)
    }
}


impl<'a> PartialEq for SmallStr<'a> {
    fn eq(&self, x: &SmallStr) -> bool {
        self.as_str().eq(x.as_str())
    }
}
impl<'a> Eq for SmallStr<'a> {}

impl PartialEq for SmallString {
    fn eq(&self, x: &SmallString) -> bool {
        self.as_str().eq(x.as_str())
    }
}
impl Eq for SmallString {}

impl<'a> std::hash::Hash for SmallStr<'a> {
    fn hash<H:std::hash::Hasher>(&self, x: &mut H) {
        self.as_str().hash(x)
    }
}

impl std::hash::Hash for SmallString {
    fn hash<H:std::hash::Hasher>(&self, x: &mut H) {
        self.as_str().hash(x)
    }
}

impl<'a> PartialOrd for SmallStr<'a> {
    fn partial_cmp(&self, x: &SmallStr) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(x.as_str())
    }
}
impl<'a> Ord for SmallStr<'a> {
    fn cmp(&self, x: &SmallStr) -> std::cmp::Ordering {
        self.as_str().cmp(x.as_str())
    }
}



impl<'a> std::fmt::Debug for SmallStr<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_str().fmt(fmt)
    }
}

impl SmallString {
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn from_str(s: &str) -> Self {
        let mut b = SmallString {
            len: s.len() as u8,
            str: [0; MAX_LENGTH],
        };
        b.clone_from_str(s);
        b
    }
    pub fn clone_from_str(&mut self, s: &str) {
        self.len = s.len() as u8;
        (&mut self.str[..s.len()]).copy_from_slice(s.as_bytes());
    }
    pub fn clear(&mut self) {
        self.len = 0;
    }
    pub fn push_str(&mut self, s: &str) {
        let l = self.len as usize;
        assert!(l + s.len() <= 0xff);
        (&mut self.str[l..l + s.len()]).copy_from_slice(s.as_bytes());
        self.len += s.len() as u8;
    }

    pub fn as_small_str(&self) -> SmallStr {
        SmallStr(self as *const SmallString as *const u8,
                 std::marker::PhantomData)
    }

    pub fn as_str(&self) -> &str {
        self.as_small_str().as_str()
    }
}

impl<'a> SmallStr<'a> {

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.0) as usize }
    }

    pub fn as_str(&self) -> &'a str {
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.0.offset(1),
                                                                     *self.0 as usize))
        }
    }
    pub fn to_unsafe(&self) -> UnsafeSmallStr {
        UnsafeSmallStr(self.0)
    }
    pub unsafe fn from_unsafe(u: UnsafeSmallStr) -> Self {
        SmallStr(u.0, std::marker::PhantomData)
    }
    pub fn to_owned(&self) -> SmallString {
        SmallString::from_str(self.as_str())
    }
}

#[derive(Clone, Copy)]
pub struct UnsafeSmallStr(*const u8);
impl std::fmt::Debug for UnsafeSmallStr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        unsafe {
            SmallStr::from_unsafe(*self).fmt(fmt)
        }
    }
}

impl Representable for UnsafeSmallStr {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        unsafe {
            let len = (*self.0) as u16;
            1 + len
        }
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        std::ptr::copy(self.0, p, self.onpage_size() as usize)
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        UnsafeSmallStr(p)
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        let a = SmallStr::from_unsafe(UnsafeSmallStr(self.0));
        let b = SmallStr::from_unsafe(x);
        a.as_str().cmp(b.as_str())
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
