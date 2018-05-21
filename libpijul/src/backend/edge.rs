use super::key::*;
use super::patch_id::*;
use sanakirja::*;
use std;

bitflags! {
    /// Possible flags of edges.
    ///
    /// Possible values are `PSEUDO_EDGE`, `FOLDER_EDGE`,
    /// `PARENT_EDGE` and `DELETED_EDGE`.
    #[derive(Serialize, Deserialize)]
    pub struct EdgeFlags: u8 {
        /// A pseudo-edge, computed when applying the patch to
        /// restore connectivity, and/or mark conflicts.
        const PSEUDO_EDGE = 1;
        /// An edge encoding file system hierarchy.
        const FOLDER_EDGE = 2;
        /// An epsilon-edge, i.e. a "non-transitive" edge used to
        /// solve conflicts.
        const EPSILON_EDGE = 4;
        /// A "reverse" edge (all edges in the graph have a reverse edge).
        const PARENT_EDGE = 8;
        /// An edge whose target (if not also `PARENT_EDGE`) or
        /// source (if also `PARENT_EDGE`) is marked as deleted.
        const DELETED_EDGE = 16;
    }
}

/// The target half of an edge in the repository graph.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[repr(packed)]
pub struct Edge {
    /// Flags of this edge.
    pub flag: EdgeFlags,
    /// Target of this edge.
    pub dest: Key<PatchId>,
    /// Patch that introduced this edge (possibly as a
    /// pseudo-edge, i.e. not explicitly in the patch, but
    /// computed from it).
    pub introduced_by: PatchId,
}
impl Edge {
    /// Create an edge with the flags set to `flags`, and other
    /// parameters to 0.
    pub fn zero(flag: EdgeFlags) -> Edge {
        Edge {
            flag: flag,
            dest: ROOT_KEY.clone(),
            introduced_by: ROOT_PATCH_ID.clone(),
        }
    }
    #[doc(hidden)]
    pub fn to_unsafe(&self) -> UnsafeEdge {
        UnsafeEdge(self)
    }
    #[doc(hidden)]
    pub unsafe fn from_unsafe<'a>(p: UnsafeEdge) -> &'a Edge {
        &*p.0
    }
}

#[derive(Clone, Copy)]
pub struct UnsafeEdge(*const Edge);

impl std::fmt::Debug for UnsafeEdge {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        unsafe {
            Edge::from_unsafe(*self).fmt(fmt)
        }
    }
}

impl Representable for UnsafeEdge {
    fn alignment() -> Alignment {
        Alignment::B1
    }
    fn onpage_size(&self) -> u16 {
        std::mem::size_of::<Edge>() as u16
    }
    unsafe fn write_value(&self, p: *mut u8) {
        trace!("write_value {:?}", p);
        std::ptr::copy(self.0, p as *mut Edge, 1)
    }
    unsafe fn read_value(p: *const u8) -> Self {
        trace!("read_value {:?}", p);
        UnsafeEdge(p as *const Edge)
    }
    unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
        let a: &Edge = &*self.0;
        let b: &Edge = &*x.0;
        a.cmp(b)
    }
    type PageOffsets = std::iter::Empty<u64>;
    fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
}
