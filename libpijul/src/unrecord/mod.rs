use rand;
use backend::*;
use patch::*;
use std::collections::{HashMap, HashSet};
use Result;
use apply::find_alive::FindAlive;
mod nodes;
mod edges;
mod context_repair;

#[derive(Debug)]
struct Workspace {
    file_moves: HashSet<Key<PatchId>>,
    context_edges: HashMap<Key<PatchId>, Edge>,
}

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    pub fn unrecord(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        patch: &Patch,
    ) -> Result<bool> {
        let timestamp = self.get_patch(&branch.patches, patch_id).unwrap();
        let is_on_branch = self.del_patches(&mut branch.patches, patch_id)?;
        self.del_revpatches(&mut branch.revpatches, timestamp, patch_id)?;

        // Is the patch used in another branch?;
        let unused_in_other_branches = {
            let mut it = self.iter_branches(None).filter(|br| {
                br.name != branch.name && self.get_patch(&br.patches, patch_id).is_some()
            });
            it.next().is_none()
        };

        if is_on_branch {
            debug!("unrecord: {:?}", patch_id);

            self.unapply(branch, patch_id, patch, unused_in_other_branches)?;

            for dep in patch.dependencies().iter() {
                let internal_dep = self.get_internal(dep.as_ref()).unwrap().to_owned();
                // Test whether other branches have both this patch and `dep`.
                let other_branches_have_dep = self.iter_branches(None).any(|br| {
                    br.name != branch.name && self.get_patch(&br.patches, internal_dep).is_some()
                        && self.get_patch(&br.patches, patch_id).is_some()
                });

                if !other_branches_have_dep {
                    self.del_revdep(internal_dep, Some(patch_id))?;
                }
            }
        }


        // If no other branch uses this patch, delete from revdeps.
        if unused_in_other_branches {
            info!("deleting patch");
            // Delete all references to patch_id in revdep.
            while self.del_revdep(patch_id, None)? {}
            let ext = self.get_external(patch_id).unwrap().to_owned();
            self.del_external(patch_id)?;
            self.del_internal(ext.as_ref())?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Unrecord the patch, returning true if and only if another
    /// branch still uses this patch.
    pub fn unapply(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        patch: &Patch,
        unused_in_other_branch: bool,
    ) -> Result<()> {
        debug!("revdep: {:?}", self.get_revdep(patch_id, None));

        // Check that the branch has no patch that depends on this one.
        assert!(
            self.iter_revdep(Some((patch_id, None)))
                .take_while(|&(p, _)| p == patch_id)
                .all(|(_, p)| self.get_patch(&branch.patches, p).is_none())
        );

        let mut workspace = Workspace {
            file_moves: HashSet::new(),
            context_edges: HashMap::new(),
        };
        let mut find_alive = FindAlive::new();

        // Check applied, check dependencies.
        for change in patch.changes().iter() {
            match *change {
                Change::NewEdges {
                    ref edges,
                    previous,
                    flag,
                    ..
                } => self.unrecord_edges(
                    &mut find_alive,
                    branch,
                    patch_id,
                    patch.dependencies(),
                    previous,
                    flag,
                    edges,
                    &mut workspace,
                    unused_in_other_branch,
                )?,
                Change::NewNodes {
                    ref up_context,
                    ref down_context,
                    ref line_num,
                    ref flag,
                    ref nodes,
                    ..
                } => self.unrecord_nodes(
                    branch,
                    patch_id,
                    patch.dependencies(),
                    up_context,
                    down_context,
                    *line_num,
                    *flag,
                    nodes,
                    &mut workspace,
                    unused_in_other_branch,
                )?,
            }
        }
        Ok(())
    }

    fn reconnect_across_deleted_nodes(
        &mut self,
        branch: &mut Branch,
        dependencies: &HashSet<Hash>,
        deleted_nodes: &[Key<PatchId>],
    ) -> Result<()> {
        debug!("reconnect_across_deleted_nodes");
        let mut find_alive = FindAlive::new();
        let mut alive_ancestors = Vec::new();
        let mut files = Vec::new();

        // find alive descendants of the deleted nodes.
        let mut alive_descendants = Vec::new();
        for &c in deleted_nodes {
            debug!("down_context c = {:?}", c);
            if !self.is_alive(branch, c) {
                find_alive.clear();
                find_alive.push(c);
                self.find_alive_descendants(&mut find_alive, branch, &mut alive_descendants);
            }
        }

        if !alive_descendants.is_empty() {

            // find alive ancestors of the deleted nodes.
            for &c in deleted_nodes {
                debug!("down_context c = {:?}", c);
                if !self.is_alive(branch, c) {
                    find_alive.clear();
                    find_alive.push(c);
                    let mut first_file = None;
                    self.find_alive_ancestors(&mut find_alive, branch, &mut alive_ancestors, &mut first_file, &mut files);
                }
            }
            debug!(
                "ancestors = {:?}, descendants = {:?}",
                alive_ancestors,
                alive_descendants
            );
            // file_edges should contain the now zombie files.
            debug!("file: {:?}", files);
            for (mut k, mut v) in files.drain(..) {
                assert!(v.flag.contains(EdgeFlags::DELETED_EDGE));
                let introduced_by = self.get_external(v.introduced_by).unwrap().to_owned();
                if !dependencies.contains(&introduced_by) {
                    v.flag = (v.flag | EdgeFlags::PSEUDO_EDGE) ^ EdgeFlags::DELETED_EDGE;
                    debug!("zombie {:?} {:?}", k, v);
                    self.put_nodes(branch, k, &v)?;
                }
            }

            for ancestor in alive_ancestors.iter() {
                for descendant in alive_descendants.iter() {
                    let mut edge = Edge::zero(EdgeFlags::PSEUDO_EDGE);
                    edge.dest = *descendant;
                    debug!("adding {:?} -> {:?}", ancestor, edge);
                    self.put_nodes_with_rev(branch, *ancestor, edge)?;
                }
            }
        }
        Ok(())
    }

    fn remove_file_from_inodes(&mut self, k: Key<PatchId>) -> Result<()> {
        let inode = self.get_revinodes(k).map(|x| x.to_owned());
        if let Some(inode) = inode {
            self.del_revinodes(k, None)?;
            self.del_inodes(inode, None)?;
        }
        Ok(())
    }
}
