//! Layout of a repository (files in `.pijul`) on the disk. This
//! module exports both high-level functions that require no knowledge
//! of the repository, and lower-level constants documented on
//! [pijul.org/documentation/repository](https://pijul.org/documentation/repository),
//! used for instance for downloading files from remote repositories.

use std::path::{Path, PathBuf};
use std::fs::{metadata, create_dir_all, File};
use std::io::{Write, BufReader};
use std::collections::HashSet;
use bs58;
use std;
use backend::{Hash, HashRef, MutTxn, ROOT_INODE};
use patch::{Patch, PatchHeader};
use Result;
use flate2;
use rand;
use rand::Rng;
use ignore::WalkBuilder;
use ignore::overrides::OverrideBuilder;

/// Name of the root directory, i.e. `.pijul`.
pub const PIJUL_DIR_NAME: &'static str = ".pijul";

/// Concatenate the parameter with `PIJUL_DIR_NAME`.
pub fn repo_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    p.as_ref().join(PIJUL_DIR_NAME)
}

/// Directory where the pristine is, from the root of the repository.
/// For instance, if the repository in in `/a/b`,
/// `pristine_dir("/a/b") returns `/a/b/.pijul/pristine`.
pub fn pristine_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join("pristine");
}

/// Directory where the patches are. `patches_dir("/a/b") = "/a/b/.pijul/patches"`.
pub fn patches_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join("patches")
}

/// Basename of the changes file for branch `br`. This file is only
/// used when pulling/pushing over HTTP (where calling remote programs
/// to list patches is impossible).
///
/// The changes file contains the same information as the one returned by `pijul log --hash-only`.
pub fn branch_changes_base_path(b: &str) -> String {
    "changes.".to_string() + &bs58::encode(b.as_bytes()).into_string()
}

/// Changes file from the repository root and branch name.
pub fn branch_changes_file(p: &Path, b: &str) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join(branch_changes_base_path(b))
}

/// The meta file, where user preferences are stored.
pub fn meta_file(p: &Path) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join("meta.toml")
}

/// The id file is used for remote operations, to identify a
/// repository and save bandwidth when the remote state is partially
/// known.
pub fn id_file(p: &Path) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join("id")
}

/// Find the repository root from one of its descendant
/// directories. Return `None` iff `dir` is not in a repository.
pub fn find_repo_root<'a>(dir: &'a Path) -> Option<PathBuf> {
    let mut p = dir.to_path_buf();
    loop {
        p.push(PIJUL_DIR_NAME);
        match metadata(&p) {
            Ok(ref attr) if attr.is_dir() => {
                p.pop();
                return Some(p);
            }
            _ => {}
        }
        p.pop();

        if !p.pop() {
            return None
        }
    }
}

#[doc(hidden)]
pub const ID_LENGTH: usize = 100;

/// Create a repository. `dir` must be the repository root (a
/// `".pijul"` directory will be created in `dir`).
pub fn create<R:Rng>(dir: &Path, mut rng: R) -> std::io::Result<()> {
    debug!("create: {:?}", dir);
    let mut repo_dir = repo_dir(dir);
    try!(create_dir_all(&repo_dir));

    repo_dir.push("pristine");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();

    repo_dir.push("patches");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();

    repo_dir.push("id");
    let mut f = std::fs::File::create(&repo_dir)?;
    let mut x = String::new();
    x.extend(rng.gen_ascii_chars().take(ID_LENGTH));
    f.write_all(x.as_bytes())?;
    repo_dir.pop();

    repo_dir.push("version");
    let mut f = std::fs::File::create(&repo_dir)?;
    writeln!(f, "{}", env!("CARGO_PKG_VERSION"))?;
    repo_dir.pop();

    repo_dir.push("local");
    create_dir_all(&repo_dir)?;
    repo_dir.pop();

    repo_dir.push("hooks");
    create_dir_all(&repo_dir)?;
    repo_dir.pop();

    repo_dir.push("local/ignore");
    let _f = std::fs::File::create(&repo_dir)?;
    repo_dir.pop();

    Ok(())
}

/// Basename of the patch corresponding to the given patch hash.
pub fn patch_file_name(hash: HashRef) -> String {
    hash.to_base58() + ".gz"
}

/// Read a complete patch.
pub fn read_patch(repo: &Path, hash: HashRef) -> Result<Patch> {
    let patch_dir = patches_dir(repo);
    let path = patch_dir.join(&patch_file_name(hash));
    debug!("read_patch, reading from {:?}", path);
    let f = File::open(path)?;
    let mut f = BufReader::new(f);
    let (_, _, patch) = Patch::from_reader_compressed(&mut f)?;
    Ok(patch)
}

/// Read a patch, but without the "changes" part, i.e. the actual
/// contents of the patch.
pub fn read_patch_nochanges(repo: &Path, hash: HashRef) -> Result<PatchHeader> {
    let patch_dir = patches_dir(repo);
    let path = patch_dir.join(&patch_file_name(hash));
    debug!("read_patch_nochanges, reading from {:?}", path);
    let f = File::open(path)?;
    let mut f = flate2::bufread::GzDecoder::new(BufReader::new(f));
    Ok(PatchHeader::from_reader_nochanges(&mut f)?)
}

/// Read a patch, but without the "changes" part, i.e. the actual
/// contents of the patch.
pub fn read_dependencies(repo: &Path, hash: HashRef) -> Result<Vec<Hash>> {
    let patch_dir = patches_dir(repo);
    let path = patch_dir.join(&patch_file_name(hash));
    debug!("read_patch_nochanges, reading from {:?}", path);
    let f = File::open(path)?;
    let mut f = flate2::bufread::GzDecoder::new(BufReader::new(f));
    Ok(Patch::read_dependencies(&mut f)?)
}

pub fn ignore_file(repo_root: &Path) -> PathBuf {
    repo_root.join(PIJUL_DIR_NAME).join("local").join("ignore")
}

pub fn untracked_files<T: rand::Rng>(txn: &MutTxn<T>, repo_root: &Path)
                                 -> HashSet<PathBuf> {
    let known_files = txn.list_files(ROOT_INODE).unwrap_or_else(|_| vec!());

    let o = OverrideBuilder::new(repo_root)
        .add("!.pijul").unwrap()
        .build().unwrap(); // we can be pretty confident these two calls will
                           // not fail as the glob is hard-coded

    let mut w = WalkBuilder::new(repo_root);
    w.git_ignore(false)
        .git_exclude(false)
        .git_global(false)
        .hidden(false);

    // add .pijul/local/ignore
    w.add_ignore(ignore_file(repo_root));
    w.overrides(o);

    let mut ret = HashSet::<PathBuf>::new();
    for f in w.build() {
        if let Ok(f) = f {
            let p = f.path();
            if p == repo_root {
                continue;
            }
            let pb = p.to_path_buf();

            if let Ok(stripped) = p.strip_prefix(repo_root) {
                if known_files.iter().any(|t| *t == stripped) {
                    continue;
                }
            }
            ret.insert(pb);
        }
    }
    ret
}
