//! Definition of patches, and a number of methods.
use flate2;
use rand;
use chrono;
use chrono::{DateTime, Utc};
use std::path::Path;
use base64;
use std::io::{BufRead, Read, Write};
use std::fs::{metadata, File, OpenOptions};
use thrussh_keys::key::KeyPair;
use thrussh_keys;
use thrussh_keys::PublicKeyBase64;
use std::collections::{HashMap, HashSet};
use std::str::from_utf8;
use std::path::PathBuf;
use std::rc::Rc;
pub type Flag = u8;
use {ErrorKind, Result};
use bincode::{deserialize, deserialize_from, serialize, Infinite};
use bs58;
use serde_json;

bitflags! {
    #[derive(Serialize, Deserialize)]
    pub struct PatchFlags: u32 {
        const TAG = 1;
    }
}

/// A patch without its signature suffix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UnsignedPatch {
    /// Header part, containing the metadata.
    pub header: PatchHeader,
    /// The dependencies of this patch.
    pub dependencies: HashSet<Hash>,
    /// The actual contents of the patch.
    pub changes: Vec<Change<ChangeContext<Hash>>>,
}

/// The definition of a patch.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Patch {
    Unsigned0,
    Signed0,
    Unsigned(UnsignedPatch),
}

impl Patch {
    /// The contents of this patch.
    pub fn changes(&self) -> &[Change<ChangeContext<Hash>>] {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref patch) => &patch.changes,
        }
    }
    pub fn changes_mut(&mut self) -> &mut Vec<Change<ChangeContext<Hash>>> {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref mut patch) => &mut patch.changes,
        }
    }
    /// The dependencies of this patch.
    pub fn dependencies(&self) -> &HashSet<Hash> {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref patch) => &patch.dependencies,
        }
    }
    pub fn dependencies_mut(&mut self) -> &mut HashSet<Hash> {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref mut patch) => &mut patch.dependencies,
        }
    }
    /// The header of this patch.
    pub fn header(&self) -> &PatchHeader {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref patch) => &patch.header,
        }
    }
    pub fn header_mut(&mut self) -> &mut PatchHeader {
        match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(ref mut patch) => &mut patch.header,
        }
    }

    /// Reads everything in this patch, but the actual contents.
    pub fn read_dependencies<R: Read>(mut r: R) -> Result<Vec<Hash>> {
        let version: u32 = deserialize_from(&mut r, Infinite)?;
        assert_eq!(version, 2);
        let _header: PatchHeader = deserialize_from(&mut r, Infinite)?;
        debug!("version: {:?}", version);
        Ok(deserialize_from(&mut r, Infinite)?)
    }
}

/// The header of a patch contains all the metadata about a patch
/// (but not the actual contents of a patch).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PatchHeader {
    pub authors: Vec<String>,
    pub name: String,
    pub description: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub flag: PatchFlags,
}

use std::ops::{Deref, DerefMut};
impl Deref for Patch {
    type Target = PatchHeader;
    fn deref(&self) -> &Self::Target {
        self.header()
    }
}
impl DerefMut for Patch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.header_mut()
    }
}

/// Options are for when this edge is between vertices introduced by
/// the current patch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEdge {
    pub from: Key<Option<Hash>>,
    pub to: Key<Option<Hash>>,
    pub introduced_by: Option<Hash>,
}

pub type ChangeContext<H> = Vec<Key<Option<H>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change<Context> {
    NewNodes {
        up_context: Context,
        down_context: Context,
        flag: EdgeFlags,
        line_num: LineId,
        nodes: Vec<Vec<u8>>,
        inode: Key<Option<Hash>>,
    },
    NewEdges {
        previous: EdgeFlags,
        flag: EdgeFlags,
        edges: Vec<NewEdge>,
        inode: Key<Option<Hash>>,
    },
}

impl PatchHeader {
    /// Reads everything in this patch, but the actual contents.
    pub fn from_reader_nochanges<R: Read>(mut r: R) -> Result<PatchHeader> {
        let version: u32 = deserialize_from(&mut r, Infinite)?;
        debug!("version: {:?}", version);
        Ok(deserialize_from(&mut r, Infinite)?)
    }
}

/// Semantic groups of changes, for interface purposes.
#[derive(Debug)]
pub enum Record<Context> {
    FileMove {
        new_name: String,
        del: Change<Context>,
        add: Change<Context>,
    },
    FileDel {
        name: String,
        del: Change<Context>,
    },
    FileAdd {
        name: String,
        add: Change<Context>,
    },
    Change {
        file: Rc<PathBuf>,
        change: Change<Context>,
        conflict_reordering: Vec<Change<Context>>,
    },
    Replace {
        file: Rc<PathBuf>,
        adds: Change<Context>,
        dels: Change<Context>,
        conflict_reordering: Vec<Change<Context>>,
    },
}

pub struct RecordIter<R, C> {
    rec: Option<R>,
    extra: Option<C>,
}

impl<Context> IntoIterator for Record<Context> {
    type IntoIter = RecordIter<Record<Context>, Change<Context>>;
    type Item = Change<Context>;
    fn into_iter(self) -> Self::IntoIter {
        RecordIter {
            rec: Some(self),
            extra: None,
        }
    }
}

impl<Context> Record<Context> {
    pub fn iter(&self) -> RecordIter<&Record<Context>, &Change<Context>> {
        RecordIter {
            rec: Some(self),
            extra: None,
        }
    }
}

impl<Context> Iterator for RecordIter<Record<Context>, Change<Context>> {
    type Item = Change<Context>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match rec {
                Record::FileMove { del, add, .. } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Record::FileDel { del: c, .. }
                | Record::FileAdd { add: c, .. }
                | Record::Change { change: c, .. } => Some(c),
                Record::Replace { adds, dels, .. } => {
                    self.extra = Some(adds);
                    Some(dels)
                }
            }
        } else {
            None
        }
    }
}

impl<'a, Context> Iterator for RecordIter<&'a Record<Context>, &'a Change<Context>> {
    type Item = &'a Change<Context>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match *rec {
                Record::FileMove {
                    ref del, ref add, ..
                } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Record::FileDel { del: ref c, .. }
                | Record::FileAdd { add: ref c, .. }
                | Record::Change { change: ref c, .. } => Some(c),
                Record::Replace {
                    ref adds, ref dels, ..
                } => {
                    self.extra = Some(adds);
                    Some(dels)
                }
            }
        } else {
            None
        }
    }
}

impl UnsignedPatch {
    pub fn empty() -> Self {
        UnsignedPatch {
            header: PatchHeader {
                authors: vec![],
                name: "".to_string(),
                description: None,
                timestamp: chrono::Utc::now(),
                flag: PatchFlags::empty(),
            },
            dependencies: HashSet::new(),
            changes: vec![],
        }
    }
    pub fn leave_unsigned(self) -> Patch {
        Patch::Unsigned(self)
    }

    fn is_tag(&self) -> bool {
        self.header.flag.contains(PatchFlags::TAG)
    }

    pub fn inverse(&self, hash: &Hash, changes: &mut Vec<Change<ChangeContext<Hash>>>) {
        for ch in self.changes.iter() {
            debug!("inverse {:?}", ch);
            match *ch {
                Change::NewNodes {
                    ref up_context,
                    flag,
                    line_num,
                    ref nodes,
                    ref inode,
                    ..
                } => {
                    let edges = up_context
                        .iter()
                        .map(|up| NewEdge {
                            from: Key {
                                patch: match up.patch {
                                    Some(ref h) => Some(h.clone()),
                                    None => Some(hash.clone()),
                                },
                                line: up.line,
                            },
                            to: Key {
                                patch: Some(hash.clone()),
                                line: line_num,
                            },
                            introduced_by: Some(hash.clone()),
                        })
                        .chain((1..nodes.len()).map(|i| NewEdge {
                            from: Key {
                                patch: Some(hash.clone()),
                                line: line_num + (i - 1),
                            },
                            to: Key {
                                patch: Some(hash.clone()),
                                line: line_num + i,
                            },
                            introduced_by: Some(hash.clone()),
                        }))
                        .collect();
                    changes.push(Change::NewEdges {
                        edges,
                        inode: inode.clone(),
                        previous: flag,
                        flag: flag ^ EdgeFlags::DELETED_EDGE,
                    })
                }
                Change::NewEdges {
                    previous,
                    flag,
                    ref edges,
                    ref inode,
                } => changes.push(Change::NewEdges {
                    previous: flag,
                    flag: previous,
                    inode: inode.clone(),
                    edges: edges
                        .iter()
                        .map(|e| NewEdge {
                            from: e.from.clone(),
                            to: e.to.clone(),
                            introduced_by: Some(hash.clone()),
                        })
                        .collect(),
                }),
            }
        }
    }
}

impl Patch {
    /// An approximate upper bound of the number of extra bytes in the
    /// database this patch might require. This depends a lot on the
    /// patch and the database, so it might be wrong.
    pub fn size_upper_bound(&self) -> usize {
        // General overhead for applying a patch; 8 pages.
        let mut size: usize = 1 << 15;
        for c in self.changes().iter() {
            match *c {
                Change::NewNodes { ref nodes, .. } => {
                    size += nodes.iter().map(|x| x.len()).sum::<usize>();
                    size += nodes.len() * 2048 // + half a page
                }
                Change::NewEdges { ref edges, .. } => size += edges.len() * 2048,
            }
        }
        size
    }

    pub fn is_tag(&self) -> bool {
        match *self {
            Patch::Unsigned(ref patch) => patch.is_tag(),
            _ => false,
        }
    }

    /// Read one patch from a gzip-compressed `BufRead`. If several
    /// patches are available in the same `BufRead`, this method can
    /// be called again.
    pub fn from_reader_compressed<R: BufRead>(r: &mut R) -> Result<(Hash, Vec<u8>, Patch)> {
        let mut rr = flate2::bufread::GzDecoder::new(r);
        let filename = {
            let filename = if let Some(header) = rr.header() {
                if let Some(filename) = header.filename() {
                    from_utf8(filename)?
                } else {
                    return Err(ErrorKind::EOF.into());
                }
            } else {
                return Err(ErrorKind::EOF.into());
            };
            if let Some(h) = Hash::from_base58(filename) {
                h
            } else {
                // Backward-compatibility with base64-encoded patch hashes.
                Hash::from_binary(&base64::decode_config(filename, base64::URL_SAFE_NO_PAD)
                    .unwrap())
                    .unwrap()
            }
        };

        let mut buf = Vec::new();
        rr.read_to_end(&mut buf)?;

        // Checking the hash.
        let patch: Patch = deserialize(&buf[..])?;
        patch.check_hash(&buf, &filename)?;

        Ok((filename, buf, patch))
    }

    fn check_hash(&self, buf: &[u8], filename: &Hash) -> Result<()> {
        let buf = match *self {
            Patch::Signed0 | Patch::Unsigned0 => {
                panic!("refusing to interact with old patch version")
            }
            Patch::Unsigned(_) => buf,
        };
        let hash = Hash::of_slice(buf)?;
        match (filename, &hash) {
            (&Hash::Sha512(ref filename), &Hash::Sha512(ref hash))
                if &filename.0[..] == &hash.0[..] =>
            {
                Ok(())
            }
            _ => Err(ErrorKind::WrongHash.into()),
        }
    }

    pub fn to_buf(&self) -> Result<(Vec<u8>, Hash)> {
        // Encoding to a buffer.
        let buf = serialize(&self, Infinite)?;
        // Hashing the buffer.
        let hash = Hash::of_slice(&buf)?;
        Ok((buf, hash))
    }

    /// Save the patch, computing the hash.
    pub fn save<P: AsRef<Path>>(&self, dir: P, key: Option<&KeyPair>) -> Result<Hash> {
        let (buf, hash) = self.to_buf()?;
        // Writing to the file.
        let h = hash.to_base58();
        let mut path = dir.as_ref().join(&h);
        path.set_extension("gz");
        if metadata(&path).is_err() {
            debug!("save, path {:?}", path);
            let f = File::create(&path)?;
            debug!("created");
            let mut w = flate2::GzBuilder::new()
                .filename(h.as_bytes())
                .write(f, flate2::Compression::best());
            w.write_all(&buf)?;
            w.finish()?;
            debug!("saved");
        }

        if let Some(key) = key {
            path.set_extension("sig");
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;

            let mut signatures: SignatureFile =
                serde_json::from_reader(&mut file).unwrap_or(SignatureFile {
                    hash: h,
                    signatures: HashMap::new(),
                });
            let signature = key.sign_detached(&hash.as_ref().to_binary())?;
            let public_key = key.public_key_base64();
            signatures
                .signatures
                .insert(public_key, bs58::encode(&signature.as_ref()).into_string());
            serde_json::to_writer(&mut file, &signatures)?;
        }
        Ok(hash)
    }

    pub fn inverse(&self, hash: &Hash, changes: &mut Vec<Change<ChangeContext<Hash>>>) {
        match *self {
            Patch::Unsigned0 => panic!("Can't reverse old patches"),
            Patch::Signed0 => panic!("Can't reverse old patches"),
            Patch::Unsigned(ref u) => u.inverse(hash, changes),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SignatureFile {
    pub hash: String,
    pub signatures: HashMap<String, String>,
}

pub fn read_signature_file(r: &mut Read) -> Result<SignatureFile> {
    let signatures: SignatureFile = serde_json::from_reader(r)?;
    // verify signatures
    for (key, sig) in signatures.signatures.iter() {
        let k = thrussh_keys::parse_public_key_base64(&key)?;
        let sig = bs58::decode(sig).into_vec()?;
        if !k.verify_detached(signatures.hash.as_bytes(), &sig) {
            return Err(ErrorKind::WrongPatchSignature.into());
        }
    }
    Ok(signatures)
}

impl SignatureFile {
    pub fn write_signature_file(&self, w: &mut Write) -> Result<()> {
        serde_json::to_writer(w, self)?;
        Ok(())
    }
}

pub struct Signatures<'a, R: Read>(
    serde_json::StreamDeserializer<'a, serde_json::de::IoRead<R>, SignatureFile>,
);

pub fn read_signatures<'a, R: Read>(r: R) -> Signatures<'a, R> {
    Signatures(serde_json::Deserializer::from_reader(r).into_iter())
}

impl<'a, R: Read> Iterator for Signatures<'a, R> {
    type Item = Result<SignatureFile>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|n| n.map_err(super::Error::from))
    }
}

pub fn read_changes(r: &mut Read) -> Result<HashMap<Hash, ApplyTimestamp>> {
    let mut s = String::new();
    r.read_to_string(&mut s)?;
    let mut result = HashMap::new();
    for l in s.lines() {
        let mut sp = l.split(':');
        match (
            sp.next().and_then(Hash::from_base58),
            sp.next().and_then(|s| s.parse().ok()),
        ) {
            (Some(h), Some(s)) => {
                result.insert(h, s);
            }
            _ => {}
        }
    }
    Ok(result)
}

pub fn read_changes_from_file<P: AsRef<Path>>(
    changes_file: P,
) -> Result<HashMap<Hash, ApplyTimestamp>> {
    let mut file = try!(File::open(changes_file));
    read_changes(&mut file)
}

impl<U: Transaction, R> GenericTxn<U, R> {
    pub fn new_patch<I: Iterator<Item = Hash>>(
        &self,
        branch: &Branch,
        authors: Vec<String>,
        name: String,
        description: Option<String>,
        timestamp: DateTime<Utc>,
        changes: Vec<Change<ChangeContext<Hash>>>,
        extra_dependencies: I,
        flag: PatchFlags,
    ) -> Patch {
        let mut dependencies = self.dependencies(branch, changes.iter());
        dependencies.extend(extra_dependencies);
        Patch::Unsigned(UnsignedPatch {
            header: PatchHeader {
                authors,
                name,
                description,
                timestamp,
                flag,
            },
            dependencies,
            changes,
        })
    }

    pub fn dependencies<'a, I: Iterator<Item = &'a Change<ChangeContext<Hash>>>>(
        &self,
        branch: &Branch,
        changes: I,
    ) -> HashSet<Hash> {
        let mut deps = HashSet::new();
        let mut zombie_deps = HashSet::new();
        for ch in changes {
            match *ch {
                Change::NewNodes {
                    ref up_context,
                    ref down_context,
                    ..
                } => for c in up_context.iter().chain(down_context.iter()) {
                    match c.patch {
                        None | Some(Hash::None) => {}
                        Some(ref dep) => {
                            debug!("dependencies (line {}) += {:?}", line!(), dep);
                            deps.insert(dep.clone());
                        }
                    }
                },
                Change::NewEdges {
                    flag, ref edges, ..
                } => {
                    for e in edges {

                        let (from, to) = if flag.contains(EdgeFlags::PARENT_EDGE) {
                            (&e.to, &e.from)
                        } else {
                            (&e.from, &e.to)
                        };

                        match from.patch {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                debug!("dependencies (line {}) += {:?}", line!(), h);
                                deps.insert(h.clone());
                                if flag.contains(EdgeFlags::DELETED_EDGE) {
                                    // Add "known patches" to
                                    // allow identifying missing
                                    // contexts.
                                    let k = Key {
                                        patch: self.get_internal(h.as_ref()).unwrap().to_owned(),
                                        line: from.line.clone(),
                                    };
                                    self.edge_context_deps(branch, k, &mut zombie_deps)
                                }
                            }
                        }
                        match to.patch {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                debug!("dependencies (line {}) += {:?}", line!(), h);
                                deps.insert(h.clone());
                                if flag.contains(EdgeFlags::DELETED_EDGE) {
                                    // Add "known patches" to
                                    // allow identifying
                                    // missing contexts.
                                    let k = Key {
                                        patch: self.get_internal(h.as_ref()).unwrap().to_owned(),
                                        line: to.line.clone(),
                                    };
                                    self.edge_context_deps(branch, k, &mut zombie_deps)
                                }
                            }
                        }
                        match e.introduced_by {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                debug!("dependencies (line {}) += {:?}", line!(), h);
                                zombie_deps.insert(h.clone());
                            }
                        }
                    }
                }
            }
        }
        let mut h = self.minimize_deps(&deps);
        for z in zombie_deps.drain() {
            h.insert(z);
        }
        h
    }

    pub fn minimize_deps(&self, deps: &HashSet<Hash>) -> HashSet<Hash> {
        debug!("minimize_deps {:?}", deps);
        let mut covered = HashSet::new();
        let mut stack = Vec::new();
        let mut seen = HashSet::new();
        for dep_ext in deps.iter() {
            // For each dependency, do a DFS.
            let dep = self.get_internal(dep_ext.as_ref()).unwrap();
            debug!("dep = {:?}", dep);
            stack.clear();
            stack.push((dep, false));
            while let Some((current, on_path)) = stack.pop() {
                // Is current already covered? (either transitively in
                // covered, or directly in deps).
                let already_covered = covered.get(&current).is_some() || (current != dep && {
                    let current_ext = self.get_external(current).unwrap();
                    deps.get(&current_ext.to_owned()).is_some()
                });
                if already_covered {
                    // We look at all patches on the current path, and
                    // mark them as covered.
                    for &(h, h_on_path) in stack.iter() {
                        if h_on_path {
                            debug!("covered: h {:?}", h);
                            covered.insert(h);
                        }
                    }
                    break;
                }
                // If we've already seen `current`, and dep is not
                // covered, we don't need to explore `current`'s
                // children.  Or, if we're coming here for the second
                // time (i.e. after exploring all children), no need to
                // explore the children again either.
                if seen.insert(current) && !on_path {
                    stack.push((current, true));

                    for (_, parent) in self.iter_revdep(Some((current, None)))
                        .take_while(|k| k.0 == current)
                    {
                        stack.push((parent, false))
                    }
                }
            }
        }

        deps.iter()
            .filter_map(|dep_ext| {
                let dep = self.get_internal(dep_ext.as_ref()).unwrap();
                if covered.get(&dep).is_none() {
                    Some(dep_ext.to_owned())
                } else {
                    None
                }
            })
            .collect()
    }

    fn edge_context_deps(&self, branch: &Branch, k: Key<PatchId>, deps: &mut HashSet<Hash>) {
        for (_, edge) in self.iter_nodes(branch, Some((k, None)))
            .take_while(|&(k_, _)| k_ == k)
            .filter(|&(_, e_)| !e_.flag.contains(EdgeFlags::PARENT_EDGE))
        {
            if let Some(ext) = self.get_external(edge.introduced_by) {
                deps.insert(ext.to_owned());
            }
        }
    }
}

use backend::*;
use sanakirja;

impl<A: sanakirja::Transaction, R> GenericTxn<A, R> {
    /// Gets the external key corresponding to the given key, returning an
    /// owned vector. If the key is just a patch internal hash, it returns the
    /// corresponding external hash.
    pub fn external_key(&self, key: &Key<PatchId>) -> Option<Key<Option<Hash>>> {
        Some(Key {
            patch: Some(self.external_hash(key.patch).to_owned()),
            line: key.line,
        })
    }

    pub fn external_hash(&self, key: PatchId) -> HashRef {
        if key == ROOT_PATCH_ID {
            ROOT_HASH.as_ref()
        } else {
            self.get_external(key).unwrap()
        }
    }

    pub(crate) fn external_hash_opt(&self, h: Option<PatchId>) -> Option<Hash> {
        h.map(|x| self.external_hash(x).to_owned())
    }

    pub(crate) fn external_key_opt(&self, h: Key<Option<PatchId>>) -> Key<Option<Hash>> {
        Key {
            line: h.line,
            patch: self.external_hash_opt(h.patch),
        }
    }

    /// Create a new internal patch id, register it in the "external" and
    /// "internal" bases, and write the result in its second argument
    /// ("result").
    pub fn new_internal(&self, ext: HashRef) -> PatchId {
        let mut result = ROOT_PATCH_ID;
        match ext {
            HashRef::None => return result,
            HashRef::Sha512(h) => result.clone_from_slice(&h[..PATCH_ID_SIZE]),
        }
        let mut first_random = PATCH_ID_SIZE;
        loop {
            if self.get_external(result).is_none() {
                break;
            }
            if first_random > 0 {
                first_random -= 1
            };
            for x in &mut result[first_random..].iter_mut() {
                *x = rand::random()
            }
        }
        result
    }
}
