use backend::*;
use patch::*;
use record::InodeUpdate;
use {ErrorKind, Result};

use std::path::{Path, PathBuf};
use std::collections::{HashSet, HashMap};
use std;
use std::fs;
use rand;
use tempdir;
use std::borrow::Cow;

#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;

#[cfg(not(windows))]
fn set_permissions(name: &Path, permissions: u16) -> Result<()> {
    let metadata = std::fs::metadata(&name)?;
    let mut current = metadata.permissions();
    debug!("setting mode for {:?} to {:?} (currently {:?})",
           name, permissions, current);
    current.set_mode(permissions as u32);
    std::fs::set_permissions(name, current)?;
    Ok(())
}

#[cfg(windows)]
fn set_permissions(_name: &Path, _permissions: u16) -> Result<()> {
    Ok(())
}

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    // Climp up the tree (using revtree).
    fn filename_of_inode(&self, inode: Inode, working_copy: &Path) -> Option<PathBuf> {
        let mut components = Vec::new();
        let mut current = inode;
        loop {
            match self.get_revtree(current) {
                Some(v) => {
                    components.push(v.basename.to_owned());
                    current = v.parent_inode.clone();
                    if current == ROOT_INODE {
                        break;
                    }
                }
                None => return None,
            }
        }
        let mut working_copy = working_copy.to_path_buf();
        for c in components.iter().rev() {
            working_copy.push(c.as_small_str().as_str());
        }
        Some(working_copy)
    }


    /// Returns the path's inode
    pub fn follow_path(&self, path: &[&str]) -> Result<Option<Inode>> {
        // follow in tree, return inode
        let mut buf = OwnedFileId {
            parent_inode: ROOT_INODE.clone(),
            basename: SmallString::from_str(""),
        };
        for p in path {
            buf.basename.clear();
            buf.basename.push_str(*p);
            // println!("follow: {:?}",buf.to_hex());
            match self.get_tree(&buf.as_file_id()) {
                Some(v) => {
                    // println!("some: {:?}",v.to_hex());
                    buf.basename.clear();
                    buf.parent_inode = v.clone()
                }
                None => {
                    // println!("none");
                    return Ok(None);
                }
            }
        }
        Ok(Some(buf.parent_inode))
    }



    /// Collect all the children of key `key` into `files`.
    pub fn collect_children(
        &mut self,
        branch: &Branch,
        path: &Path,
        key: Key<PatchId>,
        inode: Inode,
        prefixes: Option<&HashSet<PathBuf>>,
        files: &mut HashMap<PathBuf,
                            HashMap<Key<PatchId>,
                                    (Inode,
                                     FileMetadata,
                                     Key<PatchId>,
                                     Option<Inode>,
                                     bool)>
                            >) -> Result<()> {

        let e = Edge::zero(EdgeFlags::FOLDER_EDGE);
        for (_, b) in self.iter_nodes(&branch, Some((key, Some(&e))))
            .take_while(|&(k, b)| k == key && b.flag <= EdgeFlags::FOLDER_EDGE | EdgeFlags::PSEUDO_EDGE | EdgeFlags::EPSILON_EDGE) {

                debug!("b={:?}", b);
                let cont_b = self.get_contents(b.dest).unwrap();
                let (_, b_key) = self.iter_nodes(&branch, Some((b.dest, Some(&e)))).next().unwrap();
                let b_inode = self.get_revinodes(b_key.dest);

                // This is supposed to be a small string, so we can do
                // as_slice.
                if cont_b.as_slice().len() < 2 {
                    error!("cont_b {:?} b.dest {:?}", cont_b, b.dest);
                    return Err(ErrorKind::WrongFileHeader(b.dest).into())
                }
                let (perms, basename) = cont_b.as_slice().split_at(2);

                let perms = FileMetadata::from_contents(perms);
                let basename = std::str::from_utf8(basename).unwrap();
                debug!("filename: {:?} {:?}", perms, basename);
                let name = path.join(basename);
                if let Some(prefixes) = prefixes {
                    debug!("{:?} {:?}", prefixes, name);
                    if !prefixes.iter().any(|x| {
                        // are x and name related?
                        let b_inode = if let Some(b) = b_inode { b } else { return false };
                        let x_inode = if let Ok(inode) = self.find_inode(&x) { inode } else { return false };
                        self.inode_is_ancestor_of(x_inode, b_inode)
                            || self.inode_is_ancestor_of(b_inode, x_inode)
                    }) {
                        // None of the prefixes start with name, abandon this name.
                        continue
                    }
                }

                let v = files.entry(name).or_insert(HashMap::new());
                if v.get(&b.dest).is_none() {

                    let is_zombie = {
                        let e = Edge::zero(EdgeFlags::FOLDER_EDGE|EdgeFlags::PARENT_EDGE|EdgeFlags::DELETED_EDGE);
                        self.iter_nodes(&branch, Some((b_key.dest, Some(&e))))
                            .take_while(|&(k, b)| k == b_key.dest && b.flag == e.flag)
                            .next().is_some()
                    };
                    debug!("is_zombie = {:?}", is_zombie);
                    v.insert(b.dest, (inode, perms, b_key.dest, b_inode, is_zombie));
                }
            }
        Ok(())
    }

    /// Collect names of files with conflicts
    ///
    /// As conflicts have an internal representation, it can be determined
    /// exactly which files contain conflicts.
    pub fn list_conflict_files(&mut self,
                               branch_name: &str,
                               prefixes: Option<&HashSet<PathBuf>>) -> Result<Vec<PathBuf>> {
        let mut files = HashMap::new();
        let mut next_files = HashMap::new();
        let branch = self.open_branch(branch_name)?;
        self.collect_children(&branch, "".as_ref(), ROOT_KEY, ROOT_INODE, prefixes, &mut files)?;

        let mut ret = vec!();
        let mut forward = Vec::new();
        while !files.is_empty() {
            next_files.clear();
            for (a, b) in files.drain() {
                for (_, (_, meta, inode_key, inode, is_zombie)) in b {
                    // Only bother with existing files
                    if let Some(inode) = inode {
                        if is_zombie {
                            ret.push(a.clone())
                        }
                        if meta.is_dir() {
                            self.collect_children(&branch, &a, inode_key, inode, prefixes, &mut next_files)?;
                        } else {
                            let mut graph = self.retrieve(&branch, inode_key);
                            let mut buf = std::io::sink();
                            if self.output_file(&branch, &mut buf, &mut graph, &mut forward) ?{
                                ret.push(a.clone())
                            }
                        }
                    }
                }
            }
            std::mem::swap(&mut files, &mut next_files);
        }
        Ok(ret)
    }


    fn make_conflicting_name(&self, name: &mut PathBuf, name_key: Key<PatchId>) {
        let basename = {
            let basename = name.file_name().unwrap().to_string_lossy();
            format!("{}.{}", basename, &name_key.patch.to_base58())
        };
        name.set_file_name(&basename);
    }

    fn output_alive_files(&mut self, branch: &mut Branch, prefixes: Option<&HashSet<PathBuf>>, working_copy: &Path) -> Result<()> {
        debug!("working copy {:?}", working_copy);
        let mut files = HashMap::new();
        let mut next_files = HashMap::new();
        self.collect_children(branch, "".as_ref(), ROOT_KEY, ROOT_INODE, prefixes, &mut files)?;

        let mut done = HashSet::new();

        while !files.is_empty() {
            debug!("files {:?}", files);
            next_files.clear();
            for (a, b) in files.drain() {
                let b_len = b.len();
                for (name_key, (parent_inode, meta, inode_key, inode, is_zombie)) in b {

                    /*let has_several_names = {
                        let e = Edge::zero(EdgeFlags::PARENT_EDGE | EdgeFlags::FOLDER_EDGE);
                        let mut it = self.iter_nodes(branch, Some((inode_key, Some(&e))))
                            .take_while(|&(k, v)| {
                                k == inode_key && v.flag|EdgeFlags::PSEUDO_EDGE == e.flag|EdgeFlags::PSEUDO_EDGE
                            });
                        it.next();
                        it.next().is_some()
                    };*/
                    if !done.insert(inode_key) {
                        debug!("already done {:?}", inode_key);
                        continue
                    }

                    let mut name = if b_len > 1 /*|| has_several_names*/ {
                        // debug!("b_len = {:?}, has_several_names {:?}", b_len, has_several_names);
                        let mut name = a.clone();
                        self.make_conflicting_name(&mut name, name_key);
                        Cow::Owned(name)
                    } else {
                        Cow::Borrowed(&a)
                    };
                    let file_name = name.file_name().unwrap().to_string_lossy();
                    let file_id = OwnedFileId {
                        parent_inode: parent_inode,
                        basename: SmallString::from_str(&file_name)
                    };
                    let working_copy_name = working_copy.join(name.as_ref());

                    let status = if is_zombie {
                        FileStatus::Zombie
                    } else {
                        FileStatus::Ok
                    };

                    let inode = if let Some(inode) = inode {
                        // If the file already exists, find its
                        // current name and rename it if that name
                        // is different.
                        if let Some(ref current_name) = self.filename_of_inode(inode, "".as_ref()) {
                            if current_name != name.as_ref() {
                                let current_name = working_copy.join(current_name);
                                debug!("renaming {:?} to {:?}", current_name, working_copy_name);
                                let parent = self.get_revtree(inode).unwrap().to_owned();
                                self.del_revtree(inode, None)?;
                                self.del_tree(&parent.as_file_id(), None)?;

                                debug!("file_id: {:?}", file_id);
                                if let Some(p) = working_copy_name.parent() {
                                    std::fs::create_dir_all(p)?
                                }
                                if let Err(e) = std::fs::rename(&current_name, &working_copy_name) {
                                    error!("while renaming {:?} to {:?}: {:?}", current_name, working_copy_name, e)
                                }
                            }
                        }
                        self.put_tree(&file_id.as_file_id(), inode)?;
                        self.put_revtree(inode, &file_id.as_file_id())?;
                        // If the file had been marked for deletion, remove that mark.
                        if let Some(header) = self.get_inodes(inode) {
                            debug!("header {:?}", header);
                            let mut header = header.to_owned();
                            header.status = status;
                            self.replace_inodes(inode, header)?;
                        } else {
                            let header = FileHeader {
                                key: inode_key,
                                metadata: meta,
                                status,
                            };
                            debug!("no header {:?}", header);
                            self.replace_inodes(inode, header)?;
                            self.replace_revinodes(inode_key, inode)?;
                        }
                        inode
                    } else {
                        // Else, create new inode.
                        let inode = self.create_new_inode();
                        let file_header = FileHeader {
                            key: inode_key,
                            metadata: meta,
                            status,
                        };
                        self.replace_inodes(inode, file_header)?;
                        self.replace_revinodes(inode_key, inode)?;
                        debug!("file_id: {:?}", file_id);
                        self.put_tree(&file_id.as_file_id(), inode)?;
                        self.put_revtree(inode, &file_id.as_file_id())?;
                        inode
                    };
                    if meta.is_dir() {
                        // This is a directory, register it in inodes/trees.
                        std::fs::create_dir_all(&working_copy_name)?;
                        self.collect_children(branch, &name, inode_key, inode, prefixes, &mut next_files)?;
                    } else {
                        // Output file.
                        debug!("creating file {:?}", &name);
                        let mut f = std::fs::File::create(&working_copy_name).unwrap();
                        debug!("done");

                        let mut l = self.retrieve(branch, inode_key);
                        let mut forward = Vec::new();
                        self.output_file(branch, &mut f, &mut l, &mut forward)?;
                        self.remove_redundant_edges(branch, &forward)?
                    }

                    set_permissions(&working_copy_name, meta.permissions())?
                }
            }
            std::mem::swap(&mut files, &mut next_files);
        }
        Ok(())
    }


    pub fn output_repository_assuming_no_pending_patch(&mut self,
                                                       prefixes: Option<&HashSet<PathBuf>>,
                                                       branch: &mut Branch,
                                                       working_copy: &Path,
                                                       pending_patch_id: PatchId)
                                                       -> Result<()> {


        debug!("inodes: {:?}",
               self.iter_inodes(None)
               .map(|(u, v)| (u.to_owned(), v.to_owned()))
               .collect::<Vec<_>>()
        );
        // Now, garbage collect dead inodes.
        let dead: Vec<_> = self.iter_tree(None)
            .filter_map(|(k, v)| {
                if let Some(key) = self.get_inodes(v) {
                    if key.key.patch == pending_patch_id || self.is_alive_or_zombie(branch, key.key) {
                        // Don't delete.
                        None
                    } else {
                        Some((k.to_owned(), v, self.filename_of_inode(v, working_copy)))
                    }
                } else {
                    Some((k.to_owned(), v, None))
                }
            })
            .collect();
        debug!("dead: {:?}", dead);


        // Now, "kill the deads"
        for (ref parent, inode, ref name) in dead {
            self.remove_inode_rec(inode)?;
            debug!("removed");
            if let Some(ref name) = *name {
                debug!("deleting {:?}", name);
                if let Ok(meta) = fs::metadata(name) {
                    if let Err(e) = if meta.is_dir() {
                        fs::remove_dir_all(name)
                    } else {
                        fs::remove_file(name)
                    } {
                        error!("while deleting {:?}: {:?}", name, e);
                    }
                }
            } else {
                self.del_tree(&parent.as_file_id(), Some(inode))?;
                self.del_revtree(inode, Some(&parent.as_file_id()))?;
            }
        }
        debug!("done deleting dead files");
        // Then output alive files. This has to be done *after*
        // removing files, because we a file removed might have the
        // same name as a file added without there being a conflict
        // (depending on the relation between the two patches).
        self.output_alive_files(branch, prefixes, working_copy)?;
        debug!("done raw_output_repository");
        Ok(())
    }

    fn remove_inode_rec(&mut self, inode: Inode) -> Result<()> {
        // Remove the inode from inodes/revinodes.
        let mut to_kill = vec![inode];
        while let Some(inode) = to_kill.pop() {
            debug!("kill dead {:?}", inode.to_hex());
            let header = self.get_inodes(inode).map(|x| x.to_owned());
            if let Some(header) = header {
                self.del_inodes(inode, None)?;
                self.del_revinodes(header.key, None)?;
                let mut kills = Vec::new();
                // Remove the inode from tree/revtree.
                for (k, v) in self.iter_revtree(Some((inode, None)))
                    .take_while(|&(k, _)| k == inode) {
                        kills.push((k.clone(), v.to_owned()))
                    }
                for &(k, ref v) in kills.iter() {
                    self.del_tree(&v.as_file_id(), Some(k))?;
                    self.del_revtree(k, Some(&v.as_file_id()))?;
                }
                // If the dead is a directory, remove its descendants.
                let inode_fileid = OwnedFileId {
                    parent_inode: inode.clone(),
                    basename: SmallString::from_str(""),
                };
                to_kill.extend(
                    self.iter_tree(Some((&inode_fileid.as_file_id(), None)))
                        .take_while(|&(ref k, _)| k.parent_inode == inode)
                        .map(|(_, v)| v.to_owned())
                )
            }
        }
        Ok(())
    }

    pub fn output_repository(
        &mut self,
        branch_name: &str,
        working_copy: &Path,
        prefixes: Option<&HashSet<PathBuf>>,
        pending: &Patch,
        local_pending: &HashSet<InodeUpdate>
    ) -> Result<()> {
        debug!("begin output repository {:?}", prefixes);

        debug!("applying pending patch");
        let tempdir = tempdir::TempDir::new("pijul")?;
        let hash = pending.save(tempdir.path(), None)?;
        let internal = self.apply_local_patch(branch_name, working_copy, &hash, pending, local_pending, true)?;

        debug!("applied");
        let mut branch = self.open_branch(branch_name)?;
        self.output_repository_assuming_no_pending_patch(prefixes, &mut branch, working_copy, internal)?;

        debug!("unrecording pending patch");
        self.unrecord(&mut branch, internal, pending)?;
        self.commit_branch(branch)?;
        Ok(())
    }

    pub fn output_repository_no_pending(
        &mut self,
        branch_name: &str,
        working_copy: &Path,
        prefixes: Option<&HashSet<PathBuf>>
    ) -> Result<()> {
        debug!("begin output repository {:?}", prefixes);

        let mut branch = self.open_branch(branch_name)?;
        self.output_repository_assuming_no_pending_patch(prefixes, &mut branch, working_copy, ROOT_PATCH_ID)?;
        self.commit_branch(branch)?;
        Ok(())
    }
}
