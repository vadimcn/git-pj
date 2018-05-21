use backend::*;
use Result;
use patch::*;
use rand;
use std::path::Path;
use record::{RecordState, InodeUpdate};
use std::collections::HashSet;

pub mod find_alive;
mod repair_deleted_context;
mod apply;

impl<U: Transaction, R> GenericTxn<U, R> {

    /// Return the patch id corresponding to `e`, or `internal` if `e==None`.
    pub fn internal_hash(&self, e: &Option<Hash>, internal: PatchId) -> PatchId {
        match *e {
            Some(Hash::None) => ROOT_PATCH_ID.clone(),
            Some(ref h) => self.get_internal(h.as_ref()).unwrap().to_owned(),
            None => internal.clone(),
        }
    }

    /// Fetch the internal key for this external key (or `internal` if
    /// `key.patch` is `None`).
    pub fn internal_key(&self, key: &Key<Option<Hash>>, internal: PatchId) -> Key<PatchId> {
        // debug!("internal_key: {:?} {:?}", key, internal);
        Key {
            patch: self.internal_hash(&key.patch, internal),
            line: key.line.clone(),
        }
    }

    pub fn internal_key_unwrap(&self, key: &Key<Option<Hash>>) -> Key<PatchId> {
        Key {
            patch: self.get_internal(key.patch.as_ref().unwrap().as_ref()).unwrap().to_owned(),
            line: key.line.clone(),
        }
    }
}

impl<'env, T: rand::Rng> MutTxn<'env, T> {

    /// Assumes all patches have been downloaded. The third argument
    /// `remote_patches` needs to contain at least all the patches we
    /// want to apply, and the fourth one `local_patches` at least all
    /// the patches the other repository doesn't have.
    pub fn apply_patches<F>(
        &mut self,
        branch_name: &str,
        r: &Path,
        remote_patches: &Vec<(Hash, Patch)>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, &Hash),
    {
        let mut branch = self.open_branch(branch_name)?;
        let mut new_patches_count = 0;
        for &(ref p, ref patch) in remote_patches.iter() {
            debug!("apply_patches: {:?}", p);
            self.apply_patches_rec(
                &mut branch,
                remote_patches,
                p,
                patch,
                &mut new_patches_count,
            )?;
            f(new_patches_count, p);
        }
        debug!("{} patches applied", new_patches_count);

        let (pending, local_pending) = {
            let mut record = RecordState::new();
            self.record(&mut record, branch_name, &r, None)?;
            let (changes, local) = record.finish();
            let mut p = UnsignedPatch::empty();
            p.changes = changes.into_iter().flat_map(|x| x.into_iter()).map(|x| self.globalize_change(x)).collect();
            p.dependencies = self.dependencies(&branch, p.changes.iter());
            (p.leave_unsigned(), local)
        };

        if new_patches_count > 0 {
            self.output_changes_file(&branch, r)?;
            self.commit_branch(branch)?;
            debug!("output_repository");
            self.output_repository(
                branch_name,
                &r,
                None,
                &pending,
                &local_pending,
            )?;
            debug!("done outputting_repository");
        } else {
            // The branch needs to be committed in all cases to avoid
            // leaks.
            self.commit_branch(branch)?;
        }
        debug!("finished apply_patches");
        Ok(())
    }

    /// Lower-level applier. This function only applies patches as
    /// found in `patches_dir`, following dependencies recursively. It
    /// outputs neither the repository nor the "changes file" of the
    /// branch, necessary to exchange patches locally or over HTTP.
    pub fn apply_patches_rec(&mut self,
                             branch: &mut Branch,
                             patches: &[(Hash, Patch)],
                             patch_hash: &Hash,
                             patch: &Patch,
                             new_patches_count: &mut usize)
                             -> Result<()> {

        let internal = {
            if let Some(internal) = self.get_internal(patch_hash.as_ref()) {
                if self.get_patch(&branch.patches, internal).is_some() {
                    debug!("get_patch returned {:?}", self.get_patch(&branch.patches, internal));
                    None
                } else {
                    // Doesn't have patch, but the patch is known in
                    // another branch
                    Some(internal.to_owned())
                }
            } else {
                // The patch is totally new to the repository.
                let internal = self.new_internal(patch_hash.as_ref());
                Some(internal)
            }
        };
        if let Some(internal) = internal {

            info!("Now applying patch {:?} {:?} to branch {:?}", patch.name, patch_hash, branch);
            if patch.dependencies().is_empty() {
                info!("Patch {:?} has no dependencies", patch_hash);
            }
            for dep in patch.dependencies().iter() {
                info!("Applying dependency {:?}", dep);
                info!("dep hash {:?}", dep.to_base58());
                let is_applied = {
                    if let Some(dep_internal) = self.get_internal(dep.as_ref()) {
                        self.get_patch(&branch.patches, dep_internal).is_some()
                    } else {
                        false
                    }
                };
                if !is_applied {
                    info!("Not applied");
                    // If `patches` is sorted in topological order,
                    // this shouldn't happen, because the dependencies
                    // have been applied before.
                    if let Some(&(_, ref patch)) = patches.iter().find(|&&(ref a, _)| a == dep) {
                        try!(self.apply_patches_rec(branch,
                                                    patches,
                                                    &dep,
                                                    patch,
                                                    new_patches_count));
                    } else {
                        error!("Dependency not found");
                    }
                } else {
                    info!("Already applied");
                }
                let dep_internal = self.get_internal(dep.as_ref()).unwrap().to_owned();
                self.put_revdep(dep_internal, internal)?;
            }

            // Sanakirja doesn't let us insert the same pair twice.
            self.put_external(internal, patch_hash.as_ref())?;
            debug!("sanakirja put internal {:?} {:?}", patch_hash, internal);
            self.put_internal(patch_hash.as_ref(), internal)?;

            let now = branch.apply_counter;
            branch.apply_counter += 1;
            self.apply(branch, &patch, internal, now)?;

            *new_patches_count += 1;

            Ok(())
        } else {
            info!("Patch {:?} has already been applied", patch_hash);
            Ok(())
        }
    }

    /// Apply a patch from a local record: register it, give it a hash, and then apply.
    pub fn apply_local_patch(&mut self,
                             branch_name: &str,
                             working_copy: &Path,
                             hash: &Hash,
                             patch: &Patch,
                             inode_updates: &HashSet<InodeUpdate>,
                             is_pending: bool)
                             -> Result<PatchId> {

        info!("registering a patch with {} changes",
              patch.changes().len());
        info!("dependencies: {:?}",
              patch.dependencies());
        let mut branch = self.open_branch(branch_name)?;

        // let child_patch = patch.clone();

        let internal: PatchId = self.new_internal(hash.as_ref());

        for dep in patch.dependencies().iter() {
            let dep_internal = self.get_internal(dep.as_ref()).unwrap().to_owned();
            self.put_revdep(dep_internal, internal)?;
        }
        self.put_external(internal, hash.as_ref())?;
        self.put_internal(hash.as_ref(), internal)?;

        info!("Applying local patch");
        let now = branch.apply_counter;
        self.apply(&mut branch, &patch, internal, now)?;
        debug!("synchronizing tree: {:?}", inode_updates);
        for update in inode_updates.iter() {

            self.update_inode(&branch, internal, update)?;
        }
        debug!("committing branch");
        if !is_pending {
            debug!("not pending, adding to changes");
            branch.apply_counter += 1;
            self.output_changes_file(&branch, working_copy)?;
        }
        self.commit_branch(branch)?;
        trace!("done apply_local_patch");
        Ok(internal)
    }

    /// Update the inodes/revinodes, tree/revtrees databases with the
    /// patch we just applied. This is because files don't really get
    /// moved or deleted before we apply the patch, they are just
    /// "marked as moved/deleted". This function does the actual
    /// update.
    fn update_inode(&mut self, branch: &Branch, internal: PatchId, update: &InodeUpdate) -> Result<()>{
        match *update {
            InodeUpdate::Add { ref line, ref meta, inode } => {
                let key = FileHeader {
                    metadata: *meta,
                    status: FileStatus::Ok,
                    key: Key {
                        patch: internal.clone(),
                        line: line.clone(),
                    },
                };
                // If this file addition was actually recorded.
                if self.get_nodes(&branch, key.key, None).is_some() {
                    debug!("it's in here!: {:?} {:?}", key, inode);
                    self.replace_inodes(inode, key)?;
                    self.replace_revinodes(key.key, inode)?;
                }
            },
            InodeUpdate::Deleted { inode } => {
                // If this change was actually applied.
                debug!("deleted: {:?}", inode);
                let header = self.get_inodes(inode).unwrap().clone();
                debug!("deleted header: {:?}", header);
                let edge = Edge::zero(EdgeFlags::PARENT_EDGE|EdgeFlags::FOLDER_EDGE|EdgeFlags::DELETED_EDGE);
                if self.iter_nodes(&branch, Some((header.key, Some(&edge))))
                    .take_while(|&(k,v)| k == header.key && edge.flag == v.flag)
                    .any(|(_, v)| v.introduced_by == internal)
                {
                    self.del_inodes(inode, Some(header))?;
                    self.del_revinodes(header.key, Some(inode))?;

                    // We might have killed the parent in the same
                    // update.
                    if let Some(parent) = self.get_revtree(inode).map(|x| x.to_owned()) {
                        let parent = parent.as_file_id();
                        self.del_tree(&parent, None)?;
                        self.del_revtree(inode, None)?;
                    }
                }
            },
            InodeUpdate::Moved { inode } => {
                // If this change was actually applied.
                debug!("moved: {:?}", inode);
                let mut header = self.get_inodes(inode).unwrap().clone();
                debug!("moved header: {:?}", header);
                let edge = Edge::zero(EdgeFlags::PARENT_EDGE|EdgeFlags::FOLDER_EDGE);
                if self.iter_nodes(&branch, Some((header.key, Some(&edge))))
                    .take_while(|&(k, v)| k == header.key && edge.flag == v.flag)
                    .any(|(_, v)| v.introduced_by == internal)
                {
                    header.status = FileStatus::Ok;
                    self.replace_inodes(inode, header)?;
                    self.replace_revinodes(header.key, inode)?;
                }
            },
        }
        Ok(())
    }
}
