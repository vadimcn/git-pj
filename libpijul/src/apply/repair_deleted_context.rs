use backend::*;
use Result;
use patch::*;
use rand;
use apply::find_alive::*;
use std::collections::HashSet;

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    #[doc(hidden)]
    /// Deleted contexts are conflicts. Reconnect the graph by
    /// inserting pseudo-edges alongside deleted edges.
    pub fn repair_deleted_contexts(
        &mut self,
        branch: &mut Branch,
        patch: &Patch,
        patch_id: PatchId,
    ) -> Result<()> {
        let mut alive = Vec::new();
        let mut files = Vec::new();
        let mut find_alive = FindAlive::new();
        // repair_missing_context adds all zombie edges needed.
        for ch in patch.changes().iter() {
            match *ch {
                Change::NewEdges {
                    flag, ref edges, ..
                } => {
                    if !flag.contains(EdgeFlags::DELETED_EDGE) {
                        self.repair_context_nondeleted(
                            branch,
                            patch_id,
                            edges,
                            flag,
                            &mut find_alive,
                            &mut alive,
                            &mut files
                        )?
                    } else {
                        self.repair_context_deleted(
                            branch,
                            patch_id,
                            edges,
                            flag,
                            &mut find_alive,
                            &mut alive,
                            &mut files,
                            patch.dependencies(),
                        )?
                    }
                }
                Change::NewNodes {
                    ref up_context,
                    ref down_context,
                    flag,
                    ..
                } => {
                    debug!("repairing missing contexts for newnodes");
                    // If not all lines in `up_context` are alive, this
                    // is a conflict, repair.
                    for c in up_context {
                        let c = self.internal_key(c, patch_id);

                        // Is the up context deleted by another patch, and the
                        // deletion was not also confirmed by this patch?

                        let up_context_deleted = self.was_context_deleted(branch, patch_id, c);
                        debug!(
                            "up_context_deleted: patch_id = {:?} context = {:?} up_context_deleted = {:?}",
                            patch_id, c, up_context_deleted
                        );
                        if up_context_deleted {
                            self.repair_missing_up_context(
                                &mut find_alive,
                                branch,
                                c,
                                flag,
                                &mut alive,
                                &mut files,
                                &[patch_id]
                            )?
                        }
                    }
                    // If not all lines in `down_context` are alive,
                    // this is a conflict, repair.
                    for c in down_context {
                        let c = self.internal_key(c, patch_id);
                        let down_context_deleted = self.was_context_deleted(branch, patch_id, c);
                        debug!("down_context_deleted: {:?}", down_context_deleted);
                        if down_context_deleted {
                            self.repair_missing_down_context(
                                &mut find_alive,
                                branch,
                                c,
                                &mut alive,
                                &[patch_id]
                            )?
                        }
                    }
                    debug!("apply: newnodes, done");
                }
            }
        }
        Ok(())
    }

    /// This function handles the case where we're adding an alive
    /// edge, and the origin or destination (or both) of this edge is
    /// dead in the graph.
    fn repair_context_nondeleted(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        edges: &[NewEdge],
        flag: EdgeFlags,
        find_alive: &mut FindAlive,
        alive: &mut Vec<Key<PatchId>>,
        files: &mut Vec<(Key<PatchId>, Edge)>,
    ) -> Result<()> {
        debug!("repairing missing contexts for non-deleted edges");
        for e in edges {
            let (up_context, down_context) = if flag.contains(EdgeFlags::PARENT_EDGE) {
                (
                    self.internal_key(&e.to, patch_id),
                    self.internal_key(&e.from, patch_id),
                )
            } else {
                (
                    self.internal_key(&e.from, patch_id),
                    self.internal_key(&e.to, patch_id),
                )
            };
            if self.was_context_deleted(branch, patch_id, up_context) {
                self.repair_missing_up_context(find_alive, branch, up_context, flag, alive, files, &[patch_id])?
            }
            if self.was_context_deleted(branch, patch_id, down_context) {
                self.repair_missing_down_context(find_alive, branch, down_context, alive, &[patch_id])?
            }
        }
        Ok(())
    }

    /// Handle the case where we're inserting a deleted edge, but the
    /// source or target (or both) does not know about (at least one
    /// of) its adjacent edges.
    fn repair_context_deleted(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        edges: &[NewEdge],
        flag: EdgeFlags,
        find_alive: &mut FindAlive,
        alive: &mut Vec<Key<PatchId>>,
        files: &mut Vec<(Key<PatchId>, Edge)>,
        dependencies: &HashSet<Hash>,
    ) -> Result<()> {
        debug!("repairing missing contexts for deleted edges");
        debug_assert!(flag.contains(EdgeFlags::DELETED_EDGE));

        for e in edges {
            let dest = if flag.contains(EdgeFlags::PARENT_EDGE) {
                self.internal_key(&e.from, patch_id)
            } else {
                self.internal_key(&e.to, patch_id)
            };

            debug!("dest = {:?}", dest);

            // If there is at least one unknown child, repair the
            // context.
            let mut unknown_children = Vec::new();
            for (k, v) in self.iter_nodes(branch, Some((dest, None))) {
                if k != dest || v.flag | EdgeFlags::FOLDER_EDGE > EdgeFlags::PSEUDO_EDGE | EdgeFlags::EPSILON_EDGE | EdgeFlags::FOLDER_EDGE {
                    break
                }
                if v.introduced_by != patch_id && {
                    let ext = self.external_hash(v.introduced_by).to_owned();
                    !dependencies.contains(&ext)
                } {

                    unknown_children.push(v.introduced_by)
                }

                debug!("child is_unknown({}): {:?} {:?}", line!(), v, unknown_children);
            }

            if !unknown_children.is_empty() {
                self.repair_missing_up_context(find_alive, branch, dest, flag, alive, files, &unknown_children)?;
            }

            // If there is at least one alive parent we don't know
            // about, repair.
            let e = Edge::zero(EdgeFlags::PARENT_EDGE);
            unknown_children.clear();
            let mut unknown_parents = unknown_children;
            for (k, v) in self.iter_nodes(branch, Some((dest, Some(&e)))) {
                if k != dest || v.flag | EdgeFlags::FOLDER_EDGE != EdgeFlags::PARENT_EDGE | EdgeFlags::FOLDER_EDGE {
                    break
                }
                if v.introduced_by != patch_id && {
                    let ext = self.external_hash(v.introduced_by).to_owned();
                    !dependencies.contains(&ext)
                } {
                    unknown_parents.push(v.introduced_by)
                }
                debug!("parent is_unknown({}): {:?} {:?}", line!(), v, unknown_parents);
            }

            if !unknown_parents.is_empty() {
                self.repair_missing_down_context(find_alive, branch, dest, alive, &unknown_parents)?
            }

        }
        Ok(())
    }


    /// Was `context` deleted by patches other than `patch_id`, and
    /// additionally not deleted by `patch_id`?
    fn was_context_deleted(
        &self,
        branch: &Branch,
        patch_id: PatchId,
        context: Key<PatchId>,
    ) -> bool {
        let mut context_deleted = false;
        let e = Edge::zero(EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE);
        for (_, v) in self.iter_nodes(branch, Some((context, Some(&e))))
            .take_while(|&(k, v)| {
                k == context && v.flag.contains(EdgeFlags::PARENT_EDGE|EdgeFlags::DELETED_EDGE)
            }) {
                debug!("was_context_deleted {:?}", v);
            if v.introduced_by == patch_id {
                return false
            } else {
                context_deleted = true;
            }
        }
        context_deleted
    }

    /// Checks whether a line in the up context of a hunk is marked
    /// deleted, and if so, reconnect the alive parts of the graph,
    /// marking this situation as a conflict.
    pub(crate) fn repair_missing_up_context(
        &mut self,
        find_alive: &mut FindAlive,
        branch: &mut Branch,
        context: Key<PatchId>,
        flag: EdgeFlags,
        alive: &mut Vec<Key<PatchId>>,
        files: &mut Vec<(Key<PatchId>, Edge)>,
        unknown_patches: &[PatchId]
    ) -> Result<()> {
        // The up context needs a repair iff it's deleted.

        // The up context was deleted, so the alive
        // component of the graph might be disconnected, and needs
        // a repair.

        // Follow all paths upwards (in the direction of
        // DELETED_EDGE|PARENT_EDGE) until finding an alive
        // ancestor, and turn them all into zombie edges.
        find_alive.clear();
        find_alive.push(context);
        alive.clear();
        files.clear();
        let mut first_file = None;
        self.find_alive_ancestors(find_alive, branch, alive, &mut first_file, files);
        debug!("files {:?} alive {:?}", files, alive);
        if !flag.contains(EdgeFlags::FOLDER_EDGE) {
            for ancestor in alive.drain(..).chain(first_file.into_iter()) {
                let mut edge = Edge::zero(EdgeFlags::PSEUDO_EDGE | EdgeFlags::PARENT_EDGE);
                edge.dest = ancestor;
                for patch_id in unknown_patches {
                    edge.introduced_by = patch_id.clone();
                    debug!("repairing up context: {:?} {:?}", context, edge);
                    self.put_nodes_with_rev(branch, context, edge)?;
                }
            }
        }
        for (key, mut edge) in files.drain(..) {
            if !self.is_connected(branch, key, edge.dest) {
                edge.flag = EdgeFlags::PSEUDO_EDGE | EdgeFlags::PARENT_EDGE | EdgeFlags::FOLDER_EDGE;
                for patch_id in unknown_patches {
                    edge.introduced_by = patch_id.clone();
                    debug!("file: repairing up context: {:?} {:?}", key, edge);
                    self.put_nodes_with_rev(branch, key, edge)?;
                }
            }
        }
        Ok(())
    }

    /// Checks whether a line in the down context of a hunk is marked
    /// deleted, and if so, reconnect the alive parts of the graph,
    /// marking this situation as a conflict.
    fn repair_missing_down_context(
        &mut self,
        find_alive: &mut FindAlive,
        branch: &mut Branch,
        context: Key<PatchId>,
        alive: &mut Vec<Key<PatchId>>,
        unknown_patches: &[PatchId],
    ) -> Result<()> {
        // Find all alive descendants, as well as the paths
        // leading to them, and double these edges with
        // pseudo-edges everywhere.
        find_alive.clear();
        alive.clear();
        let e = Edge::zero(EdgeFlags::DELETED_EDGE);
        for (_, v) in self.iter_nodes(branch, Some((context, Some(&e))))
            .take_while(|&(k, v)| k == context && v.flag == e.flag)
        {
            find_alive.push(v.dest)
        }
        self.find_alive_descendants(find_alive, &branch, alive);
        debug!("down context relatives: {:?}", alive);
        let mut edge = Edge::zero(EdgeFlags::PSEUDO_EDGE);
        for key in alive.drain(..) {
            if !self.is_connected(branch, key, edge.dest) {
                edge.dest = key;
                edge.flag = EdgeFlags::PSEUDO_EDGE | (edge.flag & EdgeFlags::FOLDER_EDGE);
                for patch_id in unknown_patches {
                    edge.introduced_by = patch_id.clone();
                    debug!("repairing down context: {:?} {:?}", key, edge);
                    self.put_nodes_with_rev(branch, context, edge)?;
                }
            }
        }
        Ok(())
    }
}
