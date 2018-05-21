use backend::*;
use patch::*;
use {ErrorKind, Result};
use graph;
use optimal_diff;

use std::path::{Path, PathBuf};
use std::fs::metadata;
use std;
use std::io::BufRead;
use rand;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;
use std::io::Read;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashSet;

#[cfg(not(windows))]
fn permissions(attr: &std::fs::Metadata) -> Option<usize> {
    Some(attr.permissions().mode() as usize)
}
#[cfg(windows)]
fn permissions(_: &std::fs::Metadata) -> Option<usize> {
    None
}


fn file_metadata(path: &Path) -> Result<FileMetadata> {
    let attr = metadata(&path)?;
    let permissions = permissions(&attr).unwrap_or(0o755);
    debug!("permissions = {:?}", permissions);
    Ok(FileMetadata::new(permissions & 0o777, attr.is_dir()))
}

impl<U: Transaction, R> GenericTxn<U, R> {
    pub fn globalize_change(
        &self,
        change: Change<Rc<RefCell<ChangeContext<PatchId>>>>,
    ) -> Change<ChangeContext<Hash>> {

        match change {

            Change::NewNodes {
                up_context,
                down_context,
                flag,
                line_num,
                nodes,
                inode,
            } => {
                Change::NewNodes {
                    up_context: Rc::try_unwrap(up_context)
                        .unwrap()
                        .into_inner()
                        .iter()
                        .map(|&k| self.external_key_opt(k))
                        .collect(),
                    down_context: Rc::try_unwrap(down_context)
                        .unwrap()
                        .into_inner()
                        .iter()
                        .map(|&k| self.external_key_opt(k))
                        .collect(),
                    flag,
                    line_num,
                    nodes,
                    inode,
                }
            }
            Change::NewEdges {
                previous,
                flag,
                edges,
                inode,
            } => {
                Change::NewEdges {
                    previous,
                    flag,
                    edges,
                    inode,
                }
            }
        }
    }
    pub fn globalize_record(
        &self,
        change: Record<Rc<RefCell<ChangeContext<PatchId>>>>,
    ) -> Record<ChangeContext<Hash>> {

        match change {
            Record::FileMove { new_name, del, add } => Record::FileMove {
                new_name,
                del: self.globalize_change(del),
                add: self.globalize_change(add),
            },
            Record::FileDel { name, del } => Record::FileDel {
                name,
                del: self.globalize_change(del),
            },
            Record::FileAdd { name, add } => Record::FileAdd {
                name,
                add: self.globalize_change(add),
            },
            Record::Change {
                file,
                change,
                conflict_reordering,
            } => Record::Change {
                file,
                change: self.globalize_change(change),
                conflict_reordering: conflict_reordering
                    .into_iter()
                    .map(|x| self.globalize_change(x))
                    .collect(),
            },
            Record::Replace {
                file,
                adds,
                dels,
                conflict_reordering,
            } => Record::Replace {
                file,
                adds: self.globalize_change(adds),
                dels: self.globalize_change(dels),
                conflict_reordering: conflict_reordering
                    .into_iter()
                    .map(|x| self.globalize_change(x))
                    .collect(),
            },
        }
    }
}

pub struct RecordState {
    line_num: LineId,
    updatables: HashSet<InodeUpdate>,
    actions: Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>,
    redundant: Vec<(Key<PatchId>, Edge)>,
}

/// An account of the files that have been added, moved or deleted, as
/// returned by record, and used by apply (when applying a patch
/// created locally) to update the trees and inodes databases.
#[derive(Debug, Hash, PartialEq, Eq)]
pub enum InodeUpdate {
    Add {
        /// `LineId` in the new patch.
        line: LineId,
        /// `FileMetadata` in the updated file.
        meta: FileMetadata,
        /// `Inode` added by this file addition.
        inode: Inode,
    },
    Moved {
        /// `Inode` of the moved file.
        inode: Inode,
    },
    Deleted {
        /// `Inode` of the deleted file.
        inode: Inode,
    },
}

#[derive(Debug)]
pub enum WorkingFileStatus {
    Moved {
        from: FileMetadata,
        to: FileMetadata,
    },
    Deleted,
    Ok,
    Zombie,
}

fn is_text(x: &[u8]) -> bool {
    x.iter().take(8000).all(|&c| c != 0)
}

impl<'env, R: rand::Rng> MutTxn<'env, R> {
    /// Create appropriate NewNodes for adding a file.
    fn record_file_addition(
        &self,
        st: &mut RecordState,
        current_inode: Inode,
        parent_node: Key<Option<PatchId>>,
        realpath: &mut std::path::PathBuf,
        basename: &str,
    ) -> Result<Option<LineId>> {


        let name_line_num = st.line_num.clone();
        let blank_line_num = st.line_num + 1;
        st.line_num += 2;

        debug!("metadata for {:?}", realpath);
        let meta = match file_metadata(&realpath) {
            Ok(metadata) => metadata,
            Err(e) => return Err(e),
        };

        let mut name = Vec::with_capacity(basename.len() + 2);
        name.write_metadata(meta).unwrap(); // 2 bytes.
        name.extend(basename.as_bytes());

        let mut nodes = Vec::new();

        st.updatables.insert(InodeUpdate::Add {
            line: blank_line_num.clone(),
            meta: meta,
            inode: current_inode.clone(),
        });
        let up_context_ext = Key {
            patch: if parent_node.line.is_root() {
                Some(Hash::None)
            } else if let Some(patch_id) = parent_node.patch {
                Some(self.external_hash(patch_id).to_owned())
            } else {
                None
            },
            line: parent_node.line.clone(),
        };
        let up_context = Key {
            patch: if parent_node.line.is_root() {
                Some(ROOT_PATCH_ID)
            } else if let Some(patch_id) = parent_node.patch {
                Some(patch_id)
            } else {
                None
            },
            line: parent_node.line.clone(),
        };
        st.actions.push(Record::FileAdd {
            name: realpath.to_string_lossy().to_string(),
            add: Change::NewNodes {
                up_context: Rc::new(RefCell::new(vec![up_context])),
                line_num: name_line_num,
                down_context: Rc::new(RefCell::new(vec![])),
                nodes: vec![name, vec![]],
                flag: EdgeFlags::FOLDER_EDGE,
                inode: up_context_ext.clone(),
            },
        });
        // Reading the file
        if !meta.is_dir() {
            nodes.clear();

            let mut node = Vec::new();
            {
                let mut f = std::fs::File::open(realpath.as_path())?;
                f.read_to_end(&mut node)?;
            }

            let up_context = Key {
                patch: None,
                line: blank_line_num.clone(),
            };
            let up_context_ext = Key {
                patch: None,
                line: blank_line_num.clone(),
            };
            if is_text(&node) {
                let mut line = Vec::new();
                let mut f = &node[..];
                loop {
                    match f.read_until('\n' as u8, &mut line) {
                        Ok(l) => {
                            if l > 0 {
                                nodes.push(line.clone());
                                line.clear()
                            } else {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let len = nodes.len();
                if !nodes.is_empty() {
                    st.actions.push(Record::Change {
                        change: Change::NewNodes {
                            up_context: Rc::new(RefCell::new(vec![up_context])),
                            line_num: st.line_num,
                            down_context: Rc::new(RefCell::new(vec![])),
                            nodes: nodes,
                            flag: EdgeFlags::empty(),
                            inode: up_context_ext,
                        },
                        file: Rc::new(realpath.clone()),
                        conflict_reordering: Vec::new(),
                    });
                }
                st.line_num += len;
            } else {
                st.actions.push(Record::Change {
                    change: Change::NewNodes {
                        up_context: Rc::new(RefCell::new(vec![up_context])),
                        line_num: st.line_num,
                        down_context: Rc::new(RefCell::new(vec![])),
                        nodes: vec![node],
                        flag: EdgeFlags::empty(),
                        inode: up_context_ext,
                    },
                    file: Rc::new(realpath.clone()),
                    conflict_reordering: Vec::new(),
                });
                st.line_num += 1;
            }
            Ok(None)
        } else {
            Ok(Some(blank_line_num))
        }
    }

    /// Diff for binary files, doesn't both splitting the file in
    /// lines. This is wasteful, but doesn't break the format, and
    /// doesn't create conflicts inside binary files.
    fn diff_with_binary(
        &self,
        inode: Key<Option<Hash>>,
        branch: &Branch,
        st: &mut RecordState,
        ret: &mut graph::Graph,
        path: Rc<PathBuf>,
    ) -> Result<()> {

        let mut lines_b = Vec::new();
        {
            debug!("opening file for diff: {:?}", path);
            let mut f = std::fs::File::open(path.as_ref())?;
            f.read_to_end(&mut lines_b)?;
        }
        let lines = if is_text(&lines_b) {
            optimal_diff::read_lines(&lines_b)
        } else {
            vec![&lines_b[..]]
        };

        self.diff(
            inode,
            branch,
            path,
            &mut st.line_num,
            &mut st.actions,
            &mut st.redundant,
            ret,
            &lines,
        )
    }

    fn record_moved_file(
        &self,
        branch: &Branch,
        realpath: &mut std::path::PathBuf,
        st: &mut RecordState,
        parent_node: Key<Option<PatchId>>,
        current_node: Key<PatchId>,
        basename: &str,
        new_meta: FileMetadata,
        old_meta: FileMetadata,
    ) -> Result<()> {
        debug!("record_moved_file: parent_node={:?}", parent_node);
        // Delete all former names.
        let mut edges = Vec::new();
        // Now take all grandparents of l2, delete them.

        let mut name = Vec::with_capacity(basename.len() + 2);
        name.write_metadata(new_meta).unwrap();
        name.extend(basename.as_bytes());
        for parent in self.iter_parents(branch, current_node, EdgeFlags::FOLDER_EDGE) {
            debug!("iter_parents: {:?}", parent);
            let previous_name: &[u8] = match self.get_contents(parent.dest) {
                None => &[],
                Some(n) => n.as_slice(),
            };
            let name_changed = (&previous_name[2..] != &name[2..]) ||
                (new_meta != old_meta && cfg!(not(windows)));

            for grandparent in self.iter_parents(branch, parent.dest, EdgeFlags::FOLDER_EDGE) {
                debug!("iter_parents: grandparent = {:?}", grandparent);
                let grandparent_changed = if let Some(ref parent_node_patch) = parent_node.patch {
                    *parent_node_patch != grandparent.dest.patch ||
                        parent_node.line != grandparent.dest.line
                } else {
                    true
                };
                if grandparent_changed || name_changed {
                    edges.push(NewEdge {
                        from: Key {
                            line: parent.dest.line.clone(),
                            patch: Some(self.external_hash(parent.dest.patch).to_owned()),
                        },
                        to: Key {
                            line: grandparent.dest.line.clone(),
                            patch: Some(self.external_hash(grandparent.dest.patch).to_owned()),
                        },
                        introduced_by: Some(
                            self.external_hash(grandparent.introduced_by).to_owned(),
                        ),
                    })
                }
            }
        }
        debug!("edges:{:?}", edges);
        let up_context_ext = Key {
            patch: if parent_node.line.is_root() {
                Some(Hash::None)
            } else if let Some(parent_patch) = parent_node.patch {
                Some(self.external_hash(parent_patch).to_owned())
            } else {
                None
            },
            line: parent_node.line.clone(),
        };
        let up_context = Key {
            patch: if parent_node.line.is_root() {
                Some(ROOT_PATCH_ID)
            } else if let Some(parent_patch) = parent_node.patch {
                Some(parent_patch)
            } else {
                None
            },
            line: parent_node.line.clone(),
        };
        if !edges.is_empty() {
            // If this file's name or meta info has changed.
            st.actions.push(Record::FileMove {
                new_name: realpath.to_string_lossy().to_string(),
                del: Change::NewEdges {
                    edges: edges,
                    previous: EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE,
                    flag: EdgeFlags::DELETED_EDGE | EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE,
                    inode: up_context_ext.clone(),
                },
                add: Change::NewNodes {
                    up_context: Rc::new(RefCell::new(vec![up_context])),
                    line_num: st.line_num,
                    down_context: Rc::new(RefCell::new(vec![
                        Key {
                            patch: Some(current_node.patch),
                            line: current_node.line.clone(),
                        },
                    ])),
                    nodes: vec![name],
                    flag: EdgeFlags::FOLDER_EDGE,
                    inode: up_context_ext.clone(),
                },
            });
            st.line_num += 1;
        }
        if !old_meta.is_dir() {
            info!("retrieving");
            let mut ret = self.retrieve(branch, current_node);
            debug!("diff");
            let patch_ext = self.get_external(current_node.patch).unwrap();
            self.diff_with_binary(
                Key {
                    patch: Some(patch_ext.to_owned()),
                    line: current_node.line,
                },
                branch,
                st,
                &mut ret,
                Rc::new(realpath.clone()),
            )?;
        };
        Ok(())
    }

    fn record_deleted_file(
        &self,
        st: &mut RecordState,
        branch: &Branch,
        realpath: &Path,
        current_node: Key<PatchId>,
    ) -> Result<()> {
        debug!("record_deleted_file");
        let mut edges = Vec::new();
        let mut previous = EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE;
        // Now take all grandparents of the current node, delete them.
        for parent in self.iter_parents(branch, current_node, EdgeFlags::FOLDER_EDGE) {
            for grandparent in self.iter_parents(branch, parent.dest, EdgeFlags::FOLDER_EDGE) {
                edges.push(NewEdge {
                    from: self.external_key(&parent.dest).unwrap(),
                    to: self.external_key(&grandparent.dest).unwrap(),
                    introduced_by: Some(self.external_hash(grandparent.introduced_by).to_owned()),
                });
                previous = grandparent.flag;
            }
        }
        // Delete the file recursively
        let mut file_edges = vec![];
        {
            debug!("del={:?}", current_node);
            let ret = self.retrieve(branch, current_node);
            debug!("ret {:?}", ret);
            for l in ret.lines.iter() {
                if l.key != ROOT_KEY {
                    let ext_key = self.external_key(&l.key).unwrap();
                    debug!("ext_key={:?}", ext_key);
                    for v in self.iter_parents(branch, l.key, EdgeFlags::empty()) {

                        debug!("v={:?}", v);
                        file_edges.push(NewEdge {
                            from: ext_key.clone(),
                            to: self.external_key(&v.dest).unwrap(),
                            introduced_by: Some(self.external_hash(v.introduced_by).to_owned()),
                        });
                        if let Some(inode) = self.get_revinodes(v.dest) {
                            st.updatables.insert(InodeUpdate::Deleted {
                                inode: inode.to_owned()
                            });
                        }
                    }
                    for v in self.iter_parents(branch, l.key, EdgeFlags::FOLDER_EDGE) {

                        debug!("v={:?}", v);
                        edges.push(NewEdge {
                            from: ext_key.clone(),
                            to: self.external_key(&v.dest).unwrap(),
                            introduced_by: Some(self.external_hash(v.introduced_by).to_owned()),
                        });
                    }
                }
            }
        }

        if !edges.is_empty() {
            st.actions.push(Record::FileDel {
                name: realpath.to_string_lossy().to_string(),
                del: Change::NewEdges {
                    edges: edges,
                    previous,
                    flag: EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE,
                    inode: self.external_key(&current_node).unwrap(),
                },
            });
        }
        if !file_edges.is_empty() {
            st.actions.push(Record::Change {
                change: Change::NewEdges {
                    edges: file_edges,
                    previous: EdgeFlags::PARENT_EDGE,
                    flag: EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE,
                    inode: self.external_key(&current_node).unwrap(),
                },
                file: Rc::new(realpath.to_path_buf()),
                conflict_reordering: Vec::new(),
            });
        }
        Ok(())
    }

    fn record_children(
        &self,
        branch: &Branch,
        st: &mut RecordState,
        path: &mut std::path::PathBuf,
        current_node: Key<Option<PatchId>>,
        current_inode: Inode,
        obsolete_inodes: &mut Vec<Inode>,
    ) -> Result<()> {
        debug!("children of current_inode {}", current_inode.to_hex());
        let file_id = OwnedFileId {
            parent_inode: current_inode.clone(),
            basename: SmallString::from_str(""),
        };
        debug!("iterating tree, starting from {:?}", file_id.as_file_id());
        for (k, v) in self.iter_tree(Some((&file_id.as_file_id(), None)))
            .take_while(|&(ref k, _)| k.parent_inode == current_inode)
        {
            debug!("calling record_all recursively, {}", line!());

            if k.basename.len() > 0 {
                // If this is an actual file and not just the "."
                self.record_inode(
                    branch,
                    st,
                    current_node.clone(), // parent
                    v, // current_inode
                    path,
                    obsolete_inodes,
                    k.basename.as_str(),
                )?
            }
        }
        Ok(())
    }

    /// If `inode` is a file known to the current branch, return
    /// whether it's been moved, deleted, or its "status" (including
    /// permissions) has been changed.
    ///
    /// Returns `None` if `inode` is not known to the current branch.
    fn inode_status(&self, inode: Inode, path: &Path) -> (Option<(WorkingFileStatus, FileHeader)>) {
        match self.get_inodes(inode) {
            Some(file_header) => {
                let old_meta = file_header.metadata;
                let new_meta = file_metadata(path).ok();

                debug!("current_node={:?}", file_header);
                debug!("old_attr={:?},int_attr={:?}", old_meta, new_meta);

                let status = match (new_meta, file_header.status) {
                    (Some(new_meta), FileStatus::Moved) => WorkingFileStatus::Moved {
                        from: old_meta,
                        to: new_meta,
                    },
                    (Some(new_meta), _) if old_meta != new_meta => WorkingFileStatus::Moved {
                        from: old_meta,
                        to: new_meta,
                    },
                    (None, _) |
                    (_, FileStatus::Deleted) => WorkingFileStatus::Deleted,
                    (Some(_), FileStatus::Ok) => WorkingFileStatus::Ok,
                    (Some(_), FileStatus::Zombie) => WorkingFileStatus::Zombie,
                };
                Some((status, file_header.clone()))
            }
            None => None,
        }
    }


    fn record_inode(
        &self,
        branch: &Branch,
        st: &mut RecordState,
        parent_node: Key<Option<PatchId>>,
        current_inode: Inode,
        realpath: &mut std::path::PathBuf,
        obsolete_inodes: &mut Vec<Inode>,
        basename: &str,
    ) -> Result<()> {
        realpath.push(basename);
        debug!("realpath: {:?}", realpath);
        debug!("inode: {:?}", current_inode);
        debug!("header: {:?}", self.get_inodes(current_inode));
        let status_header = self.inode_status(current_inode, realpath);
        debug!("status_header: {:?}", status_header);
        let mut current_key = match &status_header {
            &Some((_, ref file_header)) => {
                Some(Key {
                    patch: Some(file_header.key.patch.clone()),
                    line: file_header.key.line.clone(),
                })
            }
            &None => None,
        };

        match status_header {
            Some((WorkingFileStatus::Moved {
                      from: old_meta,
                      to: new_meta,
                  },
                  file_header)) => {
                st.updatables.insert(InodeUpdate::Moved {
                    inode: current_inode.clone(),
                });
                self.record_moved_file(
                    branch,
                    realpath,
                    st,
                    parent_node,
                    file_header.key,
                    basename,
                    new_meta,
                    old_meta,
                )?
            }
            Some((WorkingFileStatus::Deleted, file_header)) => {
                st.updatables.insert(InodeUpdate::Deleted {
                    inode: current_inode.clone()
                });
                self.record_deleted_file(
                    st,
                    branch,
                    realpath,
                    file_header.key,
                )?
            }
            Some((WorkingFileStatus::Ok, file_header)) => {
                if !file_header.metadata.is_dir() {
                    let mut ret = self.retrieve(branch, file_header.key);
                    debug!("now calling diff {:?}", file_header.key);
                    let inode = Key {
                        patch: Some(self.external_hash(file_header.key.patch).to_owned()),
                        line: file_header.key.line,
                    };
                    self.confirm_path(st, branch, &realpath, file_header.key)?;
                    self.diff_with_binary(
                        inode,
                        branch,
                        st,
                        &mut ret,
                        Rc::new(realpath.clone()),
                    )?;
                } else {
                    // Confirm
                    self.confirm_path(st, branch, &realpath, file_header.key)?;
                }
            }
            Some((WorkingFileStatus::Zombie, _)) => {
                // This file is a zombie, but the user has not
                // specified anything to do with this file, so leave
                // it alone.
            }
            None => {
                if let Ok(new_key) = self.record_file_addition(
                    st,
                    current_inode,
                    parent_node,
                    realpath,
                    basename,
                )
                {
                    current_key = new_key.map(|next| {
                        Key {
                            patch: None,
                            line: next,
                        }
                    })
                } else {
                    obsolete_inodes.push(current_inode)
                }
            }

        }

        let current_key = current_key;
        debug!("current_node={:?}", current_key);
        if let Some(current_node) = current_key {
            self.record_children(
                branch,
                st,
                realpath,
                current_node,
                current_inode,
                obsolete_inodes,
            )?;
        };
        realpath.pop();
        Ok(())
    }


    fn external_newedge(&self, from: Key<PatchId>, to: Key<PatchId>, introduced_by: PatchId) -> NewEdge {
        NewEdge {
            from: Key {
                patch: Some(self.external_hash(from.patch).to_owned()),
                line: from.line
            },
            to: Key {
                patch: Some(self.external_hash(to.patch).to_owned()),
                line: to.line
            },
            introduced_by: Some(self.external_hash(introduced_by).to_owned()),
        }
    }

    /// `key` must be a non-root inode key.
    fn confirm_path(&self, st: &mut RecordState, branch: &Branch, realpath: &Path, key: Key<PatchId>) -> Result<()> {
        debug!("confirm_path");
        let e = Edge::zero(EdgeFlags::PARENT_EDGE|EdgeFlags::FOLDER_EDGE|EdgeFlags::DELETED_EDGE);
        // Are there deleted parent edges?
        let mut edges = Vec::new();
        for (_, v) in self.iter_nodes(branch, Some((key, Some(&e))))
            .filter(|&(k, v)| k == key && v.flag == e.flag)
        {
            debug!("confirm {:?}", v.dest);
            edges.push(self.external_newedge(key, v.dest, v.introduced_by));
            for (_, v_) in self.iter_nodes(branch, Some((v.dest, Some(&e))))
                .filter(|&(k, v_)| k == v.dest && v_.flag == e.flag)
            {
                debug!("confirm 2 {:?}", v_.dest);
                edges.push(self.external_newedge(v.dest, v_.dest, v_.introduced_by));
            }
        }

        if !edges.is_empty() {
            let inode = Key {
                patch: Some(self.external_hash(key.patch).to_owned()),
                line: key.line.clone(),
            };
            st.actions.push(Record::FileAdd {
                name: realpath.to_string_lossy().to_string(),
                add: Change::NewEdges {
                    edges,
                    previous: EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE,
                    flag: EdgeFlags::FOLDER_EDGE | EdgeFlags::PARENT_EDGE,
                    inode,
                },
            });
        }
        debug!("/confirm_path");

        Ok(())
    }
}

impl RecordState {
    pub fn new() -> Self {
        RecordState {
            line_num: LineId::new() + 1,
            actions: Vec::new(),
            updatables: HashSet::new(),
            redundant: Vec::new(),
        }
    }

    pub fn finish(self) -> (Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>, HashSet<InodeUpdate>) {
        (self.actions, self.updatables)
    }
}

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    pub fn record(
        &mut self,
        state: &mut RecordState,
        branch_name: &str,
        working_copy: &std::path::Path,
        prefix: Option<&std::path::Path>,
    ) -> Result<()> {

        let branch = self.open_branch(branch_name)?;
        let mut obsolete_inodes = Vec::new();
        {
            let mut realpath = PathBuf::from(working_copy);

            if let Some(prefix) = prefix {

                realpath.extend(prefix);
                let basename = realpath.file_name().unwrap().to_str().unwrap().to_string();
                realpath.pop();
                let inode = self.find_inode(prefix)?;
                // Key needs to be the parent's node.
                let key: Key<PatchId> = {
                    // find this inode's parent.
                    if let Some(parent) = self.get_revtree(inode) {
                        if parent.parent_inode.is_root() {
                            ROOT_KEY
                        } else if let Some(key) = self.get_inodes(parent.parent_inode) {
                            key.key
                        } else {
                            return Err(ErrorKind::FileNotInRepo(prefix.to_path_buf()).into());
                        }
                    } else {
                        return Err(ErrorKind::FileNotInRepo(prefix.to_path_buf()).into());
                    }
                };
                let key = Key {
                    patch: Some(key.patch),
                    line: key.line,
                };
                self.record_inode(
                    &branch,
                    state,
                    key,
                    inode,
                    &mut realpath,
                    &mut obsolete_inodes,
                    &basename,
                )?

            } else {
                let key = Key {
                    patch: None,
                    line: LineId::new(),
                };
                self.record_children(
                    &branch,
                    state,
                    &mut realpath,
                    key,
                    ROOT_INODE,
                    &mut obsolete_inodes,
                )?
                // self.record_root(&branch, &mut st, &mut realpath)?;
            }
            debug!("record done, {} changes", state.actions.len());
            debug!("changes: {:?}", state.actions);
        }
        // try!(self.remove_redundant_edges(&mut branch, &mut st.redundant));
        self.commit_branch(branch)?;
        debug!("remove_redundant_edges done");
        for inode in obsolete_inodes {
            self.rec_delete(inode)?;
        }
        Ok(())
    }
}
