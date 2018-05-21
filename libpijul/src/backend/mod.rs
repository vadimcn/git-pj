use sanakirja;
pub use sanakirja::Transaction;
use sanakirja::Representable;
use std::path::Path;
use rand;
use std;
use {ErrorKind, Result};
use hex;

pub use self::patch_id::*;

fn from_hex(hex: &str, s: &mut [u8]) -> bool {
    let hex = hex.as_bytes();
    if hex.len() <= 2 * s.len() {
        let mut i = 0;
        while i < hex.len() {
            let h = hex[i].to_ascii_lowercase();
            if h >= b'0' && h <= b'9' {
                s[i/2] = s[i/2] << 4 | (h - b'0')
            } else if h >= b'a' && h <= b'f' {
                s[i/2] = s[i/2] << 4 | (h - b'a' + 10)
            } else {
                return false
            }
            i += 1
        }
        if i & 1 == 1 {
            s[i/2] = s[i/2] << 4
        }
        true
    } else {
        false
    }
}
mod patch_id;
mod key;
mod edge;
mod hash;
mod inode;
mod file_header;
mod file_id;
mod small_string;

pub use self::key::*;
pub use self::edge::*;
pub use self::hash::*;
pub use self::inode::*;
pub use self::file_header::*;
pub use self::file_id::*;
pub use self::small_string::*;

pub type NodesDb = sanakirja::Db<self::key::Key<PatchId>, self::edge::UnsafeEdge>;

/// The type of patch application numbers.
pub type ApplyTimestamp = u64;

/// The u64 is the epoch time in seconds when this patch was applied
/// to the repository.
type PatchSet = sanakirja::Db<self::patch_id::PatchId, ApplyTimestamp>;

type RevPatchSet = sanakirja::Db<ApplyTimestamp, self::patch_id::PatchId>;

pub struct Dbs {
    /// A map of the files in the working copy.
    tree: sanakirja::Db<self::file_id::UnsafeFileId, self::inode::Inode>,
    /// The reverse of tree.
    revtree: sanakirja::Db<self::inode::Inode, self::file_id::UnsafeFileId>,
    /// A map from inodes (in tree) to keys in branches.
    inodes: sanakirja::Db<self::inode::Inode, self::file_header::FileHeader>,
    /// The reverse of inodes, minus the header.
    revinodes: sanakirja::Db<self::key::Key<PatchId>, self::inode::Inode>,
    /// Text contents of keys.
    contents: sanakirja::Db<self::key::Key<PatchId>, sanakirja::value::UnsafeValue>,
    /// A map from external patch hashes to internal ids.
    internal: sanakirja::Db<self::hash::UnsafeHash, self::patch_id::PatchId>,
    /// The reverse of internal.
    external: sanakirja::Db<self::patch_id::PatchId, self::hash::UnsafeHash>,
    /// A reverse map of patch dependencies, i.e. (k,v) is in this map
    /// means that v depends on k.
    revdep: sanakirja::Db<self::patch_id::PatchId, self::patch_id::PatchId>,
    /// A map from branch names to graphs.
    branches: sanakirja::Db<self::small_string::UnsafeSmallStr, (NodesDb, PatchSet, RevPatchSet, u64)>,
    /// A map of edges to patches that remove them.
    cemetery: sanakirja::Db<(self::key::Key<PatchId>, self::edge::UnsafeEdge), self::patch_id::PatchId>,
}

/// Common type for both mutable transactions (`MutTxn`) and immutable
/// transaction (`Txn`). All of `Txn`'s methods are also `MutTxn`'s
/// methods.
pub struct GenericTxn<T, R> {
    #[doc(hidden)]
    pub txn: T,
    #[doc(hidden)]
    pub rng: R,
    #[doc(hidden)]
    pub dbs: Dbs,
}

/// A mutable transaction on a repository.
pub type MutTxn<'env, R> = GenericTxn<sanakirja::MutTxn<'env, ()>, R>;
/// An immutable transaction on a repository.
pub type Txn<'env> = GenericTxn<sanakirja::Txn<'env>, ()>;

/// The default name of a branch, for users who start working before
/// choosing branch names (or like the default name, "master").
pub const DEFAULT_BRANCH: &'static str = "master";

/// A repository. All operations on repositories must be done via transactions.
pub struct Repository {
    env: sanakirja::Env,
}

#[derive(Debug,PartialEq, Clone, Copy)]
pub enum Root {
    Tree,
    RevTree,
    Inodes,
    RevInodes,
    Contents,
    Internal,
    External,
    RevDep,
    Branches,
    Cemetery,
}

trait OpenDb: Transaction {
    fn open_db<K: Representable, V: Representable>(&mut self,
                                                   num: Root)
                                                   -> Result<sanakirja::Db<K, V>> {
        if let Some(db) = self.root(num as usize) {
            Ok(db)
        } else {
            Err(ErrorKind::NoDb(num).into())
        }
    }
}

impl<'a, T> OpenDb for sanakirja::MutTxn<'a, T> {
    fn open_db<K: Representable, V: Representable>(&mut self,
                                                   num: Root)
                                                   -> Result<sanakirja::Db<K, V>> {
        if let Some(db) = self.root(num as usize) {
            Ok(db)
        } else {
            Ok(self.create_db()?)
        }
    }
}
impl<'a> OpenDb for sanakirja::Txn<'a> {}

// Repositories need at least 2^5 = 32 pages, each of size 2^12.
const MIN_REPO_SIZE: u64 = 1 << 17;

impl Repository {

    #[doc(hidden)]
    pub fn size(&self) -> u64 {
        self.env.size()
    }

    #[doc(hidden)]
    pub fn repository_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        let size = sanakirja::Env::file_size(path.as_ref())?;
        debug!("repository_size = {:?}", size);
        Ok(size)
    }

    /// Open a repository, possibly increasing the size of the underlying file if `size_increase` is `Some(…)`.
    pub fn open<P: AsRef<Path>>(path: P, size_increase: Option<u64>) -> Result<Self> {
        let size =
            if let Some(size) = size_increase {
                Repository::repository_size(path.as_ref()).unwrap_or(MIN_REPO_SIZE)
                    + std::cmp::max(size, MIN_REPO_SIZE)
            } else {
                if let Ok(len) = Repository::repository_size(path.as_ref()) {
                    std::cmp::max(len, MIN_REPO_SIZE)
                } else {
                    MIN_REPO_SIZE
                }
            };
        Ok(Repository { env: try!(sanakirja::Env::new(path, size)) })
    }

    /// Open a repository, possibly increasing the size of the underlying file if `size_increase` is `Some(…)`.
    pub unsafe fn open_nolock<P: AsRef<Path>>(path: P, size_increase: Option<u64>) -> Result<Self> {
        let size =
            if let Some(size) = size_increase {
                Repository::repository_size(path.as_ref()).unwrap_or(MIN_REPO_SIZE)
                    + std::cmp::max(size, MIN_REPO_SIZE)
            } else {
                if let Ok(len) = Repository::repository_size(path.as_ref()) {
                    std::cmp::max(len, MIN_REPO_SIZE)
                } else {
                    MIN_REPO_SIZE
                }
            };
        debug!("sanakirja::Env::new_nolock");
        Ok(Repository { env: sanakirja::Env::new_nolock(path, size)? })
    }

    /// Close a repository. It is undefined behaviour to use it afterwards.
    pub unsafe fn close(&mut self) {
        self.env.close()
    }

    /// Start an immutable transaction. Immutable transactions can run
    /// concurrently.
    pub fn txn_begin(&self) -> Result<Txn> {
        let mut txn = try!(self.env.txn_begin());
        let dbs = try!(Dbs::new(&mut txn));
        let repo = GenericTxn {
            txn: txn,
            rng: (),
            dbs: dbs,
        };
        Ok(repo)
    }

    /// Start a mutable transaction. Mutable transactions exclude each
    /// other, but can in principle be run concurrently with immutable
    /// transactions. In that case, the immutable transaction only
    /// have access to the state of the repository immediately before
    /// the mutable transaction started.
    pub fn mut_txn_begin<R: rand::Rng>(&self, r: R) -> Result<MutTxn<R>> {
        let mut txn = try!(self.env.mut_txn_begin());
        let dbs = try!(Dbs::new(&mut txn));
        let repo = GenericTxn {
            txn: txn,
            rng: r,
            dbs: dbs,
        };
        Ok(repo)
    }
}

impl Dbs {
    fn new<T: OpenDb>(txn: &mut T) -> Result<Self> {
        let external = txn.open_db(Root::External)?;
        let branches = txn.open_db(Root::Branches)?;
        let tree = txn.open_db(Root::Tree)?;
        let revtree = txn.open_db(Root::RevTree)?;
        let inodes = txn.open_db(Root::Inodes)?;
        let revinodes = txn.open_db(Root::RevInodes)?;
        let internal = txn.open_db(Root::Internal)?;
        let contents = txn.open_db(Root::Contents)?;
        let revdep = txn.open_db(Root::RevDep)?;
        let cemetery = txn.open_db(Root::Cemetery)?;

        Ok(Dbs {
            external,
            branches,
            inodes,
            tree,
            revtree,
            revinodes,
            internal,
            revdep,
            contents,
            cemetery,
        })
    }
}

/// The representation of a branch. The "application number" of a
/// patch on a branch is the state of the application counter at the
/// time the patch has been applied to that branch.
#[derive(Debug)]
pub struct Branch {
    /// The table containing the branch graph.
    pub db: NodesDb,
    /// The map of all patches applied to that branch, ordered by patch hash.
    pub patches: PatchSet,
    /// The map of all patches applied to that branch, ordered by application number.
    pub revpatches: RevPatchSet,
    /// The number of patches that have been applied on that branch,
    /// including patches that are no longer on the branch (i.e. that
    /// have been unrecorded).
    pub apply_counter: u64,
    /// Branch name.
    pub name: small_string::SmallString,
}

use sanakirja::Commit;
/// Branches and commits.
impl<'env, R: rand::Rng> MutTxn<'env, R> {

    /// Open a branch by name, creating an empty branch with that name
    /// if the name doesn't exist.
    pub fn open_branch<'name>(&mut self, name: &str) -> Result<Branch> {
        let name = small_string::SmallString::from_str(name);
        let (branch, patches, revpatches, counter) = if let Some(x) = self.txn
            .get(&self.dbs.branches, name.as_small_str().to_unsafe(), None) {
                x
            } else {
                (self.txn.create_db()?, self.txn.create_db()?, self.txn.create_db()?, 0)
            };
        Ok(Branch {
            db: branch,
            patches: patches,
            revpatches: revpatches,
            name: name,
            apply_counter: counter
        })
    }

    /// Commit a branch. This is a extremely important thing to do on
    /// branches, and it is not done automatically when committing
    /// transactions.
    ///
    /// **I repeat: not calling this method before committing a
    /// transaction might cause database corruption.**
    pub fn commit_branch(&mut self, branch: Branch) -> Result<()> {
        debug!("Commit_branch. This is not too safe.");
        // Since we are replacing the value, we don't want to
        // decrement its reference counter (which del would do), hence
        // the transmute.
        //
        // This would normally be wrong. The only reason it works is
        // because we know that dbs_branches has never been forked
        // from another database, hence all the reference counts to
        // its elements are 1 (and therefore represented as "not
        // referenced" in Sanakirja.
        let mut dbs_branches: sanakirja::Db<UnsafeSmallStr, (u64, u64, u64, u64)> =
            unsafe { std::mem::transmute(self.dbs.branches) };

        debug!("Commit_branch, dbs_branches = {:?}", dbs_branches);
        self.txn.del(&mut self.rng,
                     &mut dbs_branches,
                     branch.name.as_small_str().to_unsafe(),
                     None)?;
        debug!("Commit_branch, dbs_branches = {:?}", dbs_branches);
        self.dbs.branches = unsafe { std::mem::transmute(dbs_branches) };
        self.txn.put(&mut self.rng,
                     &mut self.dbs.branches,
                     branch.name.as_small_str().to_unsafe(),
                     (branch.db, branch.patches, branch.revpatches, branch.apply_counter))?;
        debug!("Commit_branch, self.dbs.branches = {:?}", self.dbs.branches);
        Ok(())
    }

    /// Rename a branch. The branch still needs to be committed after
    /// this operation.
    pub fn rename_branch(&mut self, branch: &mut Branch, new_name: &str) -> Result<()> {
        debug!("Commit_branch. This is not too safe.");
        // Since we are replacing the value, we don't want to
        // decrement its reference counter (which del would do), hence
        // the transmute.
        //
        // Read the note in `commit_branch` to understand why this
        // works.
        let name_exists = self.get_branch(new_name).is_some();
        if name_exists {
            Err(ErrorKind::BranchNameAlreadyExists(new_name.to_string()).into())
        } else {
            let mut dbs_branches: sanakirja::Db<UnsafeSmallStr, (u64, u64, u64, u64)> =
                unsafe { std::mem::transmute(self.dbs.branches) };
            self.txn.del(&mut self.rng,
                         &mut dbs_branches,
                         branch.name.as_small_str().to_unsafe(),
                         None)?;
            self.dbs.branches = unsafe { std::mem::transmute(dbs_branches) };
            branch.name.clone_from_str(new_name);
            Ok(())
        }
    }

    /// Commit a transaction. **Be careful to commit all open branches
    /// before**.
    pub fn commit(mut self) -> Result<()> {

        self.txn.set_root(Root::Tree as usize, self.dbs.tree);
        self.txn.set_root(Root::RevTree as usize, self.dbs.revtree);
        self.txn.set_root(Root::Inodes as usize, self.dbs.inodes);
        self.txn.set_root(Root::RevInodes as usize, self.dbs.revinodes);
        self.txn.set_root(Root::Contents as usize, self.dbs.contents);
        self.txn.set_root(Root::Internal as usize, self.dbs.internal);
        self.txn.set_root(Root::External as usize, self.dbs.external);
        self.txn.set_root(Root::Branches as usize, self.dbs.branches);
        self.txn.set_root(Root::RevDep as usize, self.dbs.revdep);
        self.txn.set_root(Root::Cemetery as usize, self.dbs.cemetery);

        try!(self.txn.commit());
        Ok(())
    }
}

use sanakirja::value::*;
use sanakirja::{Cursor, RevCursor};
pub struct TreeIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeFileId, Inode>);

impl<'a, T: Transaction + 'a> Iterator for TreeIterator<'a, T> {
    type Item = (FileId<'a>, Inode);
    fn next(&mut self) -> Option<Self::Item> {
        debug!("tree iter");
        if let Some((k, v)) = self.0.next() {
            debug!("tree iter: {:?} {:?}", k, v);
            unsafe { Some((FileId::from_unsafe(k), v)) }
        } else {
            None
        }
    }
}

pub struct RevtreeIterator<'a, T: Transaction + 'a>(Cursor<'a, T, Inode, UnsafeFileId>);

impl<'a, T: Transaction + 'a> Iterator for RevtreeIterator<'a, T> {
    type Item = (Inode, FileId<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((k, FileId::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct NodesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, Key<PatchId>, UnsafeEdge>);

impl<'a, T: Transaction + 'a> Iterator for NodesIterator<'a, T> {
    type Item = (Key<PatchId>, &'a Edge);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((k, Edge::from_unsafe(v))) }
        } else {
            None
        }
    }
}
pub struct BranchIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeSmallStr, (NodesDb, PatchSet, RevPatchSet, u64)>);

impl<'a, T: Transaction + 'a> Iterator for BranchIterator<'a, T> {
    type Item = Branch;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some(Branch {
                name: SmallStr::from_unsafe(k).to_owned(),
                db: v.0,
                patches: v.1,
                revpatches: v.2,
                apply_counter: v.3
            }) }
        } else {
            None
        }
    }
}


pub struct PatchesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, ApplyTimestamp>);

impl<'a, T: Transaction + 'a> Iterator for PatchesIterator<'a, T> {
    type Item = (PatchId, ApplyTimestamp);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct RevAppliedIterator<'a, T: Transaction + 'a>(RevCursor<'a, T, ApplyTimestamp, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for RevAppliedIterator<'a, T> {
    type Item = (ApplyTimestamp, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct AppliedIterator<'a, T: Transaction + 'a>(Cursor<'a, T, ApplyTimestamp, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for AppliedIterator<'a, T> {
    type Item = (ApplyTimestamp, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}


pub struct InodesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, Inode, FileHeader>);

impl<'a, T: Transaction + 'a> Iterator for InodesIterator<'a, T> {
    type Item = (Inode, FileHeader);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct InternalIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeHash, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for InternalIterator<'a, T> {
    type Item = (HashRef<'a>, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((HashRef::from_unsafe(k), v)) }
        } else {
            None
        }
    }
}
pub struct ExternalIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, UnsafeHash>);

impl<'a, T: Transaction + 'a> Iterator for ExternalIterator<'a, T> {
    type Item = (PatchId, HashRef<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((k, HashRef::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct RevdepIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for RevdepIterator<'a, T> {
    type Item = (PatchId, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct ContentsIterator<'a, T: Transaction + 'a>(&'a T, Cursor<'a, T, Key<PatchId>, UnsafeValue>);

impl<'a, T: Transaction + 'a> Iterator for ContentsIterator<'a, T> {
    type Item = (Key<PatchId>, Value<'a, T>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.1.next() {
            unsafe { Some((k, Value::from_unsafe(&v, self.0))) }
        } else {
            None
        }
    }
}

pub struct CemeteryIterator<'a, T: Transaction + 'a>(&'a T, Cursor<'a, T, (Key<PatchId>, UnsafeEdge), PatchId>);

impl<'a, T: Transaction + 'a> Iterator for CemeteryIterator<'a, T> {
    type Item = ((Key<PatchId>, &'a Edge), PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(((k, v), w)) = self.1.next() {
            unsafe { Some(((k, Edge::from_unsafe(v)), w)) }
        } else {
            None
        }
    }
}


mod dump {
    use super::*;
    use sanakirja;

    impl<U: Transaction, R> GenericTxn<U, R> {
        pub fn dump(&self) {
            debug!("============= dumping Tree");
            for (k, v) in self.iter_tree(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Inodes");
            for (k, v) in self.iter_inodes(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping RevDep");
            for (k, v) in self.iter_revdep(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Internal");
            for (k, v) in self.iter_internal(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping External");
            for (k, v) in self.iter_external(None) {
                debug!("> {:?} {:?} {:?}", k, v, v.to_base58());
            }
            debug!("============= dumping Contents");
            {
                sanakirja::debug(&self.txn, &[&self.dbs.contents], "dump_contents");
            }
            for (k, v) in self.iter_contents(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Branches");
            for (br, (db, patches, revpatches, counter)) in self.txn.iter(&self.dbs.branches, None) {
                debug!("patches: {:?} {:?}", patches, revpatches);
                debug!("============= dumping Patches in branch {:?}, counter = {:?}", br, counter);
                for (k, v) in self.txn.iter(&patches, None) {
                    debug!("> {:?} {:?}", k, v)
                }
                debug!("============= dumping RevPatches in branch {:?}", br);
                for (k, v) in self.txn.iter(&revpatches, None) {
                    debug!("> {:?} {:?}", k, v)
                }
                debug!("============= dumping Nodes in branch {:?}", br);
                unsafe {
                    // sanakirja::debug(&self.txn, &[&db], path);
                    debug!("> {:?}", SmallStr::from_unsafe(br));
                    for (k, v) in self.txn.iter(&db, None) {
                        debug!(">> {:?} {:?}", k, Edge::from_unsafe(v))
                    }
                }
            }
        }
    }
}


pub struct ParentsIterator<'a, U: Transaction+'a> {
    it: NodesIterator<'a, U>,
    key: Key<PatchId>,
    flag: EdgeFlags
}

impl<'a, U: Transaction+'a> Iterator for ParentsIterator<'a, U> {
    type Item = &'a Edge;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((v, e)) = self.it.next() {
            if v == self.key && e.flag <= self.flag {
                Some(e)
            } else {
                None
            }
        } else {
            None
        }
    }
}
/*
macro_rules! iterate_parents {
    ($txn:expr, $branch:expr, $key:expr, $flag: expr) => { {
        let edge = Edge::zero($flag|PARENT_EDGE);
        $txn.iter_nodes(& $branch, Some(($key, Some(&edge))))
            .take_while(|&(k, parent)| {
                *k == *$key && parent.flag <= $flag|PARENT_EDGE|PSEUDO_EDGE
            })
            .map(|(_,b)| b)
    } }
}
*/

impl<U: Transaction, R> GenericTxn<U, R> {

    /// Does this repository has a branch called `name`?
    pub fn has_branch(&self, name: &str) -> bool {
        let name = small_string::SmallString::from_str(name);
        self.txn.get(&self.dbs.branches, name.as_small_str().to_unsafe(), None).is_some()
    }

    /// Get the branch with the given name, if it exists.
    pub fn get_branch<'name>(&self, name: &str) -> Option<Branch> {
        let name = small_string::SmallString::from_str(name);
        if let Some((branch, patches, revpatches, counter)) = self.txn.get(&self.dbs.branches, name.as_small_str().to_unsafe(), None) {
            Some(Branch {
                db: branch,
                patches: patches,
                revpatches: revpatches,
                apply_counter: counter,
                name: name,
            })
        } else {
            None
        }
    }

    /// Return the first edge of this `key` if `edge` is `None`, and
    /// a pointer to the edge in the database if `edge` is `Some`.
    pub fn get_nodes<'a>(&'a self,
                         branch: &Branch,
                         key: Key<PatchId>,
                         edge: Option<&Edge>)
                         -> Option<&'a Edge> {
        self.txn
            .get(&branch.db, key, edge.map(|e| e.to_unsafe()))
            .map(|e| unsafe { Edge::from_unsafe(e) })
    }

    /// An iterator over keys and edges, in branch `branch`, starting
    /// from key and edge specified by `key`. If `key` is `None`, the
    /// iterations start from the first key and first edge. If `key`
    /// is of the form `Some(a, None)`, they start from the first edge
    /// of key `a`. If `key` is of the from `Some(a, Some(b))`, they
    /// start from the first key and edge that is at least `(a, b)`.
    pub fn iter_nodes<'a>(&'a self,
                          branch: &'a Branch,
                          key: Option<(Key<PatchId>, Option<&Edge>)>)
                          -> NodesIterator<'a, U> {
        NodesIterator(self.txn.iter(&branch.db,
                                    key.map(|(k, v)| (k, v.map(|v| v.to_unsafe())))))
    }

    pub fn iter_parents<'a>(&'a self,
                            branch: &'a Branch,
                            key: Key<PatchId>,
                            flag: EdgeFlags)
                            -> ParentsIterator<'a, U> {
        let edge = Edge::zero(flag|EdgeFlags::PARENT_EDGE);
        ParentsIterator {
            it: self.iter_nodes(branch, Some((key, Some(&edge)))),
            key,
            flag: flag|EdgeFlags::PARENT_EDGE|EdgeFlags::PSEUDO_EDGE
        }
    }

    /// An iterator over branches in the database, starting from the
    /// given branch name.
    pub fn iter_branches<'a>(&'a self,
                             key: Option<&SmallStr>)
                             -> BranchIterator<'a, U> {
        BranchIterator(self.txn.iter(&self.dbs.branches, key.map(|k| (k.to_unsafe(), None))))
    }

    /// An iterator over patches in a branch, in the alphabetical
    /// order of their hash.
    pub fn iter_patches<'a>(&'a self,
                            branch: &'a Branch,
                            key: Option<PatchId>)
                            -> PatchesIterator<'a, U> {

        PatchesIterator(self.txn.iter(&branch.patches,
                                      key.map(|k| (k, None))))
    }

    /// An iterator over patches in a branch, in the reverse order in
    /// which they were applied.
    pub fn rev_iter_applied<'a>(&'a self,
                                branch: &'a Branch,
                                key: Option<ApplyTimestamp>)
                                -> RevAppliedIterator<'a, U> {

        RevAppliedIterator(self.txn.rev_iter(&branch.revpatches,
                                             key.map(|k| (k, None))))
    }

    /// An iterator over patches in a branch in the order in which
    /// they were applied.
    pub fn iter_applied<'a>(&'a self,
                            branch: &'a Branch,
                            key: Option<ApplyTimestamp>)
                            -> AppliedIterator<'a, U> {

        AppliedIterator(self.txn.iter(&branch.revpatches,
                                      key.map(|k| (k, None))))
    }

    /// An iterator over files and directories currently tracked by
    /// Pijul, starting from the given `FileId`. The `Inode`s returned
    /// by the iterator can be used to form new `FileId`s and traverse
    /// the tree from top to bottom.
    ///
    /// The set of tracked files is changed by the following
    /// operations: outputting the repository, adding, deleting and
    /// moving files. It is not related to branches, but only to the
    /// files actually present on the file system.
    pub fn iter_tree<'a>(&'a self,
                         key: Option<(&FileId, Option<Inode>)>)
                         -> TreeIterator<'a, U> {
        debug!("iter_tree: {:?}", key);
        TreeIterator(self.txn.iter(&self.dbs.tree,
                                   key.map(|(k, v)| (k.to_unsafe(), v))))
    }

    /// An iterator over files and directories, following directories
    /// in the opposite direction.
    pub fn iter_revtree<'a>(&'a self,
                            key: Option<(Inode, Option<&FileId>)>)
                            -> RevtreeIterator<'a, U> {
        RevtreeIterator(self.txn.iter(&self.dbs.revtree,
                                      key.map(|(k, v)| (k, v.map(|v| v.to_unsafe())))))
    }

    /// An iterator over the "inodes" database, which contains
    /// correspondences between files on the filesystem and the files
    /// in the graph.
    pub fn iter_inodes<'a>(&'a self,
                           key: Option<(Inode, Option<FileHeader>)>)
                           -> InodesIterator<'a, U> {
        InodesIterator(self.txn.iter(&self.dbs.inodes, key))
    }

    /// Iterator over the `PatchId` to `Hash` correspondence.
    pub fn iter_external<'a>(&'a self,
                             key: Option<(PatchId, Option<HashRef>)>)
                             -> ExternalIterator<'a, U> {
        ExternalIterator(self.txn.iter(&self.dbs.external,
                                       key.map(|(k, v)| (k, v.map(|v| v.to_unsafe())))))
    }

    /// Iterator over the `Hash` to `PatchId` correspondence.
    pub fn iter_internal<'a>(&'a self,
                             key: Option<(HashRef, Option<PatchId>)>)
                             -> InternalIterator<'a, U> {
        InternalIterator(self.txn.iter(&self.dbs.internal,
                                       key.map(|(k, v)| (k.to_unsafe(), v))))
    }

    /// Iterator over reverse dependencies (`(k, v)` is in the reverse dependency table if `v` depends on `k`, and both are in at least one branch).
    pub fn iter_revdep<'a>(&'a self,
                           key: Option<(PatchId, Option<PatchId>)>)
                           -> RevdepIterator<'a, U> {
        RevdepIterator(self.txn.iter(&self.dbs.revdep, key))
    }

    /// An iterator over line contents (common to all branches).
    pub fn iter_contents<'a>(&'a self,
                             key: Option<Key<PatchId>>)
                             -> ContentsIterator<'a, U> {
        ContentsIterator(&self.txn,
                         self.txn.iter(&self.dbs.contents, key.map(|k| (k, None))))
    }

    /// An iterator over edges in the cemetery.
    pub fn iter_cemetery<'a>(&'a self,
                              key: Key<PatchId>,
                              edge: Edge)
                              -> CemeteryIterator<'a, U> {
        CemeteryIterator(&self.txn,
                          self.txn.iter(&self.dbs.cemetery,
                                        Some(((key, edge.to_unsafe()), None))))
    }

    /// Get the `Inode` of a give `FileId`. A `FileId` is itself
    /// composed of an inode and a name, hence this can be used to
    /// traverse the tree of tracked files from top to bottom.
    pub fn get_tree<'a>(&'a self, key: &FileId) -> Option<Inode> {
        self.txn
            .get(&self.dbs.tree, key.to_unsafe(), None)
    }

    /// Get the parent `FileId` of a given `Inode`. A `FileId` is
    /// itself composed of an `Inode` and a name, so this can be used
    /// to traverse the tree of tracked files from bottom to top
    /// (starting from a leaf).
    pub fn get_revtree<'a>(&'a self, key: Inode) -> Option<FileId<'a>> {
        self.txn
            .get(&self.dbs.revtree, key, None)
            .map(|e| unsafe { FileId::from_unsafe(e) })
    }

    /// Get the key in branches for the given `Inode`, as well as
    /// meta-information on the file (permissions, and whether it has
    /// been moved or deleted compared to the branch).
    ///
    /// This table is updated every time the repository is output, and
    /// when files are moved or deleted. It is meant to be
    /// synchronised with the current branch (if any).
    pub fn get_inodes<'a>(&'a self, key: Inode) -> Option<FileHeader> {
        self.txn
            .get(&self.dbs.inodes, key, None)
    }

    /// Get the `Inode` corresponding to `key` in branches (see the
    /// documentation for `get_inodes`).
    pub fn get_revinodes(&self, key: Key<PatchId>) -> Option<Inode> {
        self.txn.get(&self.dbs.revinodes, key, None)
    }

    /// Get the contents of a line.
    pub fn get_contents<'a>(&'a self, key: Key<PatchId>) -> Option<Value<'a, U>> {
        if let Some(e) = self.txn.get(&self.dbs.contents, key, None) {
            unsafe { Some(Value::from_unsafe(&e, &self.txn)) }
        } else {
            None
        }
    }

    /// Get the `PatchId` (or internal patch identifier) of the
    /// provided patch hash.
    pub fn get_internal(&self, key: HashRef) -> Option<PatchId> {
        match key {
            HashRef::None => Some(ROOT_PATCH_ID),
            h => {
                self.txn
                    .get(&self.dbs.internal, h.to_unsafe(), None)
            }
        }
    }

    /// Get the `HashRef` (external patch identifier) of the provided
    /// internal patch identifier.
    pub fn get_external<'a>(&'a self, key: PatchId) -> Option<HashRef<'a>> {
        self.txn
            .get(&self.dbs.external, key, None)
            .map(|e| unsafe { HashRef::from_unsafe(e) })
    }

    /// Get the patch number in the branch. Patch numbers are
    /// guaranteed to always increase when a new patch is applied, but
    /// are not necessarily consecutive.
    pub fn get_patch(&self,
                     patch_set: &PatchSet,
                     patchid: PatchId)
                     -> Option<ApplyTimestamp> {
        self.txn.get(patch_set, patchid, None)
    }

    /// Get the smallest patch id that depends on `patch` (and is at
    /// least `dep` in alphabetical order if `dep`, is `Some`).
    pub fn get_revdep(&self,
                      patch: PatchId,
                      dep: Option<PatchId>)
                      -> Option<PatchId> {

        self.txn
            .get(&self.dbs.revdep,
                 patch,
                 dep)
    }

    /// Dump the graph of a branch into a writer, in dot format.
    pub fn debug<W>(&self, branch_name: &str, w: &mut W, exclude_parents: bool)
        where W: std::io::Write
    {
        debug!("debugging branch {:?}", branch_name);
        let mut styles = Vec::with_capacity(16);
        for i in 0..32 {
            let flag = EdgeFlags::from_bits(i as u8).unwrap();
            styles.push(("color=").to_string() + ["red", "blue", "orange", "green", "black"][(i >> 1) & 3] +
                        if flag.contains(EdgeFlags::DELETED_EDGE) {
                ", style=dashed"
            } else {
                ""
            } +
                        if flag.contains(EdgeFlags::PSEUDO_EDGE) {
                ", style=dotted"
            } else {
                ""
            })
        }
        w.write(b"digraph{\n").unwrap();
        let branch = self.get_branch(branch_name).unwrap();

        let mut cur: Key<PatchId> = ROOT_KEY.clone();
        for (k, v) in self.iter_nodes(&branch, None) {
            if k != cur {
                let cont = if let Some(cont) = self.get_contents(k) {
                    let cont = cont.into_cow();
                    let cont = &cont[ .. std::cmp::min(50, cont.len())];
                    format!("{:?}",
                            match std::str::from_utf8(cont) {
                                Ok(x) => x.to_string(),
                                Err(_) => hex::encode(cont),
                            })
                } else {
                    "\"\"".to_string()
                };
                // remove the leading and trailing '"'.
                let cont = &cont [1..(cont.len()-1)];
                write!(w,
                       "n_{}[label=\"{}.{}: {}\"];\n",
                       k.to_hex(),
                       k.patch.to_base58(),
                       k.line.to_hex(),
                       cont.replace("\n", "")
                )
                    .unwrap();
                cur = k.clone();
            }
            debug!("debug: {:?}", v);
            let flag = v.flag.bits();
            if !(exclude_parents && v.flag.contains(EdgeFlags::PARENT_EDGE)) {
                write!(w,
                       "n_{}->n_{}[{},label=\"{} {}\"];\n",
                       k.to_hex(),
                       &v.dest.to_hex(),
                       styles[(flag & 0xff) as usize],
                       flag,
                       v.introduced_by.to_base58()
                )
                    .unwrap();
            }
        }
        w.write(b"}\n").unwrap();
    }

    /// Dump the graph of a branch into a writer, in dot format.
    pub fn debug_folders<W>(&self, branch_name: &str, w: &mut W)
        where W: std::io::Write
    {
        debug!("debugging branch {:?}", branch_name);
        let mut styles = Vec::with_capacity(16);
        for i in 0..32 {
            let flag = EdgeFlags::from_bits(i as u8).unwrap();
            styles.push(("color=").to_string() + ["red", "blue", "orange", "green", "black"][(i >> 1) & 3] +
                        if flag.contains(EdgeFlags::DELETED_EDGE) {
                ", style=dashed"
            } else {
                ""
            } +
                        if flag.contains(EdgeFlags::PSEUDO_EDGE) {
                ", style=dotted"
            } else {
                ""
            })
        }
        w.write(b"digraph{\n").unwrap();
        let branch = self.get_branch(branch_name).unwrap();

        let mut nodes = vec![ROOT_KEY];
        while let Some(k) = nodes.pop() {
            let cont = if let Some(cont) = self.get_contents(k) {
                let cont = cont.into_cow();
                let cont = &cont[ .. std::cmp::min(50, cont.len())];
                if cont.len() > 2 {
                    let (a, b) = cont.split_at(2);
                    let cont = format!("{:?}", std::str::from_utf8(b).unwrap());
                    let cont = &cont [1..(cont.len()-1)];
                    format!("{} {}", hex::encode(a), cont)
                } else {
                    format!("{}", hex::encode(cont))
                }
            } else {
                "".to_string()
            };
            // remove the leading and trailing '"'.
            write!(w,
                   "n_{}[label=\"{}.{}: {}\"];\n",
                   k.to_hex(),
                   k.patch.to_base58(),
                   k.line.to_hex(),
                   cont.replace("\n", "")
            )
                .unwrap();

            for (_, child) in self.iter_nodes(&branch, Some((k, None)))
                .take_while(|&(k_, v_)| {
                    debug!(target:"debug", "{:?} {:?}", k_, v_);
                    k_ == k
                }) {
                let flag = child.flag.bits();
                write!(w,
                       "n_{}->n_{}[{},label=\"{} {}\"];\n",
                       k.to_hex(),
                       &child.dest.to_hex(),
                       styles[(flag & 0xff) as usize],
                       flag,
                       child.introduced_by.to_base58()
                )
                    .unwrap();
                if child.flag.contains(EdgeFlags::FOLDER_EDGE) && !child.flag.contains(EdgeFlags::PARENT_EDGE) {
                    nodes.push(child.dest)
                }
            }
        }
        w.write(b"}\n").unwrap();
    }

    /// Is there an alive/pseudo edge from `a` to `b`.
    pub fn is_connected(&self, branch: &Branch, a: Key<PatchId>, b: Key<PatchId>) -> bool {
        self.test_edge(branch, a, b, EdgeFlags::empty(), EdgeFlags::PSEUDO_EDGE|EdgeFlags::FOLDER_EDGE)
    }

    /// Is there an alive/pseudo edge from `a` to `b`.
    pub fn test_edge(&self, branch: &Branch, a: Key<PatchId>, b: Key<PatchId>, min: EdgeFlags, max: EdgeFlags) -> bool {
        debug!("is_connected {:?} {:?}", a, b);
        let mut edge = Edge::zero(min);
        edge.dest = b;
        self.iter_nodes(&branch, Some((a, Some(&edge))))
            .take_while(|&(k, v)| k == a && v.dest == b && v.flag <= max)
            .next()
            .is_some()
    }
}

/// Low-level operations on mutable transactions.
impl<'env, R: rand::Rng> MutTxn<'env, R> {

    /// Delete a branch, destroying its associated graph and patch set.
    pub fn drop_branch(&mut self, branch: &str) -> Result<bool> {
        let name = SmallString::from_str(branch);
        Ok(self.txn.del(&mut self.rng, &mut self.dbs.branches, name.as_small_str().to_unsafe(), None)?)
    }

    /// Add a binding to the graph of a branch. All edges must be
    /// inserted twice, once in each direction, and this method only
    /// inserts one direction.
    pub fn put_nodes(&mut self,
                     branch: &mut Branch,
                     key: Key<PatchId>,
                     edge: &Edge)
                     -> Result<bool> {
        debug!("put_nodes: {:?} {:?}", key, edge);
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut branch.db,
                             key,
                             edge.to_unsafe())))
    }

    /// Same as `put_nodes`, but also adds the reverse edge.
    pub fn put_nodes_with_rev(&mut self, branch: &mut Branch, mut key: Key<PatchId>, mut edge: Edge) -> Result<bool> {
        self.put_nodes(branch, key, &edge)?;
        std::mem::swap(&mut key, &mut edge.dest);
        edge.flag.toggle(EdgeFlags::PARENT_EDGE);
        self.put_nodes(branch, key, &edge)
    }

    /// Delete a binding from a graph. If `edge` is `None`, delete the
    /// smallest binding with key at least `key`.
    pub fn del_nodes(&mut self,
                     branch: &mut Branch,
                     key: Key<PatchId>,
                     edge: Option<&Edge>)
                     -> Result<bool> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut branch.db,
                             key,
                             edge.map(|e| e.to_unsafe()))))
    }

    /// Same as `del_nodes`, but also deletes the reverse edge.
    pub fn del_nodes_with_rev(&mut self, branch: &mut Branch, mut key: Key<PatchId>, mut edge: Edge) -> Result<bool> {
        self.del_nodes(branch, key, Some(&edge))?;
        std::mem::swap(&mut key, &mut edge.dest);
        edge.flag.toggle(EdgeFlags::PARENT_EDGE);
        self.del_nodes(branch, key, Some(&edge))
    }

    /// Add a file or directory into the tree database, with parent
    /// `key.parent_inode`, name `key.basename` and inode `Inode`
    /// (usually randomly generated, as `Inode`s have no relation
    /// with patches or branches).
    ///
    /// All bindings inserted here must have the reverse inserted into
    /// the revtree database. If `(key, edge)` is inserted here, then
    /// `(edge, key)` must be inserted into revtree.
    pub fn put_tree(&mut self, key: &FileId, edge: Inode) -> Result<bool> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.tree,
                             key.to_unsafe(),
                             edge)))
    }

    /// Delete a file or directory from the tree database. Similarly
    /// to the comments in the documentation of the `put_tree` method,
    /// the reverse binding must be delete from the revtree database.
    pub fn del_tree(&mut self, key: &FileId, edge: Option<Inode>) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.tree,
                        key.to_unsafe(),
                        edge)?)
    }

    /// Add a file into the revtree database (see the documentation of
    /// the `put_tree` method).
    pub fn put_revtree(&mut self, key: Inode, value: &FileId) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.revtree,
                        key,
                        value.to_unsafe())?)
    }

    /// Delete a file from the revtree database (see the documentation
    /// of the `put_tree` method).
    pub fn del_revtree(&mut self, key: Inode, value: Option<&FileId>) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.revtree,
                        key,
                        value.map(|e| e.to_unsafe()))?)
    }

    /// Delete a binding from the `inodes` database, i.e. the
    /// correspondence between branch graphs and the file tree.
    ///
    /// All bindings in inodes must have their reverse in revinodes
    /// (without the `FileMetadata`). `del_revinodes` must be called
    /// immediately before or immediately after calling this method.
    pub fn del_inodes(&mut self, key: Inode, value: Option<FileHeader>) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.inodes,
                        key,
                        value)?)
    }

    /// Replace a binding in the inodes database, or insert a new
    /// one if `key` doesn't exist yet in that database.
    ///
    /// All bindings in inodes must have their reverse inserted in
    /// revinodes (without the `FileMetadata`).
    pub fn replace_inodes(&mut self, key: Inode, value: FileHeader) -> Result<bool> {

        self.txn.del(&mut self.rng, &mut self.dbs.inodes, key, None)?;
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.inodes,
                        key,
                        value)?)
    }

    /// Replace a binding in the revinodes database, or insert a new
    /// one if `key` doesnt exist yet in that database.
    ///
    /// All bindings in revinodes must have their reverse inserted
    /// inodes (with an extra `FileMetadata`).
    pub fn replace_revinodes(&mut self, key: Key<PatchId>, value: Inode) -> Result<bool> {
        self.txn.del(&mut self.rng,
                     &mut self.dbs.revinodes,
                     key,
                     None)?;
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.revinodes,
                        key,
                        value)?)
    }

    /// Delete a binding from the `revinodes` database, i.e. the
    /// correspondence between the file tree and branch graphs.
    ///
    /// All bindings in revinodes must have their reverse in inodes
    /// (with an extra `FileMetadata`). `del_inodes` must be called
    /// immediately before or immediately after calling this method.
    pub fn del_revinodes(&mut self,
                         key: Key<PatchId>,
                         value: Option<Inode>)
                         -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.revinodes,
                        key,
                        value)?)
    }

    /// Add the contents of a line. Note that this table is common to
    /// all branches.
    pub fn put_contents(&mut self, key: Key<PatchId>, value: UnsafeValue) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.contents,
                        key,
                        value)?)
    }

    /// Remove the contents of a line.
    pub fn del_contents(&mut self,
                        key: Key<PatchId>,
                        value: Option<UnsafeValue>)
                        -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.contents,
                        key,
                        value)?)
    }



    /// Register the internal identifier of a patch. The
    /// `put_external` method must be called immediately after, or
    /// immediately before this method.
    pub fn put_internal(&mut self, key: HashRef, value: PatchId) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.internal,
                        key.to_unsafe(),
                        value)?)
    }

    /// Unregister the internal identifier of a patch. Remember to
    /// also unregister its external id.
    pub fn del_internal(&mut self, key: HashRef) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.internal,
                        key.to_unsafe(),
                        None)?)
    }

    /// Register the extern identifier of a patch. The `put_internal`
    /// method must be called immediately after, or immediately before
    /// this method.
    pub fn put_external(&mut self, key: PatchId, value: HashRef) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.external,
                        key,
                        value.to_unsafe())?)
    }

    /// Unregister the extern identifier of a patch. Remember to also
    /// unregister its internal id.
    pub fn del_external(&mut self, key: PatchId) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.external,
                        key,
                        None)?)
    }

    /// Add a patch id to a branch. This doesn't apply the patch, it
    /// only registers it as applied. The `put_revpatches` method must be
    /// called on the same branch immediately before, or immediately
    /// after.
    pub fn put_patches(&mut self, branch: &mut PatchSet, value: PatchId, time: ApplyTimestamp) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        branch,
                        value,
                        time)?)
    }

    /// Delete a patch id from a branch. This doesn't unrecord the
    /// patch, it only removes it from the patch set. The
    /// `del_revpatches` method must be called on the same branch
    /// immediately before, or immediately after.
    pub fn del_patches(&mut self, branch: &mut PatchSet, value: PatchId) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        branch,
                        value,
                        None)?)
    }

    /// Add a patch id to a branch. This doesn't apply the patch, it
    /// only registers it as applied. The `put_patches` method must be
    /// called on the same branch immediately before, or immediately
    /// after.
    pub fn put_revpatches(&mut self, branch: &mut RevPatchSet, time: ApplyTimestamp, value: PatchId) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        branch,
                        time,
                        value)?)
    }

    /// Delete a patch id from a branch. This doesn't unrecord the
    /// patch, it only removes it from the patch set. The
    /// `del_patches` method must be called on the same branch
    /// immediately before, or immediately after.
    pub fn del_revpatches(&mut self, revbranch: &mut RevPatchSet, timestamp: ApplyTimestamp, value: PatchId) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        revbranch,
                        timestamp,
                        Some(value))?)
    }

    /// Register a reverse dependency. All dependencies of all patches
    /// applied on at least one branch must be registered in this
    /// database, i.e. if a depends on b, then `(b, a)` must be
    /// inserted here.
    pub fn put_revdep(&mut self, patch: PatchId, revdep: PatchId) -> Result<bool> {
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.revdep,
                        patch,
                        revdep)?)
    }

    /// Remove a reverse dependency. Only call this method when the
    /// patch with identifier `patch` is not applied to any branch.
    pub fn del_revdep(&mut self, patch: PatchId, revdep: Option<PatchId>) -> Result<bool> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.revdep,
                        patch,
                        revdep)?)
    }

    /// Add an edge to the cemetery.
    pub fn put_cemetery(&mut self, key: Key<PatchId>, edge: &Edge, patch: PatchId) -> Result<bool> {
        let unsafe_edge = edge.to_unsafe();
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.cemetery,
                        (key, unsafe_edge),
                        patch)?)
    }

    /// Delete an edge from the cemetery.
    pub fn del_cemetery(&mut self, key: Key<PatchId>, edge: &Edge, patch: PatchId) -> Result<bool> {
        let unsafe_edge = edge.to_unsafe();
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.cemetery,
                        (key, unsafe_edge),
                        Some(patch))?)
    }

    /// Allocate a string (to be inserted in the contents database).
    pub fn alloc_value(&mut self, slice: &[u8]) -> Result<UnsafeValue> {
        Ok(UnsafeValue::alloc_if_needed(&mut self.txn, slice)?)
    }


}
