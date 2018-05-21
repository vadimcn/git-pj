use backend::*;
use Result;
use patch::*;
use rand;
use std::collections::HashSet;

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    /// Applies a patch to a repository. "new_patches" are patches that
    /// just this repository has, and the remote repository doesn't have.
    pub fn apply(
        &mut self,
        branch: &mut Branch,
        patch: &Patch,
        patch_id: PatchId,
        timestamp: ApplyTimestamp,
    ) -> Result<()> {

        assert!(self.put_patches(&mut branch.patches, patch_id, timestamp)?);
        assert!(self.put_revpatches(
            &mut branch.revpatches,
            timestamp,
            patch_id,
        )?);
        debug!("apply_raw");
        // Here we need to first apply *all* the NewNodes, and then
        // the Edges, because some of the NewNodes might be the
        // children of newly deleted edges, and we need to add the
        // corresponding pseudo-edges.
        for ch in patch.changes().iter() {
            if let Change::NewNodes {
                ref up_context,
                ref down_context,
                ref line_num,
                flag,
                ref nodes,
                ..
            } = *ch {
                assert!(!nodes.is_empty());
                debug!("apply: newnodes");
                self.add_new_nodes(
                    branch,
                    patch_id,
                    up_context,
                    down_context,
                    line_num,
                    flag,
                    nodes,
                )?;
            }
        }
        let mut parents: HashSet<Key<PatchId>> = HashSet::new();
        let mut children: HashSet<Edge> = HashSet::new();
        for ch in patch.changes().iter() {
            if let Change::NewEdges { previous, flag, ref edges, .. } = *ch {
                self.add_new_edges(
                    branch,
                    patch_id,
                    Some(previous),
                    flag,
                    edges,
                    &mut parents,
                    &mut children,
                    patch.dependencies(),
                )?;
                debug!("apply_raw:edges.done");
            }
        }

        // If there is a missing context, add pseudo-edges along the
        // edges that deleted the conflict, until finding (in both
        // directions) an alive context.
        self.repair_deleted_contexts(branch, patch, patch_id)?;

        Ok(())
    }


    /// Delete old versions of `edges`.
    fn delete_old_edge(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        previous: EdgeFlags,
        from: Key<PatchId>,
        to: Key<PatchId>,
        introduced_by: PatchId,
    ) -> Result<()> {

        debug!("delete {:?} -> {:?} ({:?}) {:?}", from, to, previous, introduced_by);
        // debug!("delete_old_edges: introduced_by = {:?}", e.introduced_by);
        let mut deleted_e = Edge {
            flag: previous,
            dest: to,
            introduced_by,
        };
        self.put_cemetery(from, &deleted_e, patch_id)?;
        if !self.del_nodes_with_rev(branch, from, deleted_e)? {
            debug!("killing pseudo instead {:?} {:?}", from, deleted_e);
            deleted_e.flag |= EdgeFlags::PSEUDO_EDGE;
            let result = self.del_nodes_with_rev(branch, from, deleted_e)?;
            debug!("killed ? {:?}", result);
        }
        Ok(())
    }

    fn add_new_edges(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        previous: Option<EdgeFlags>,
        flag: EdgeFlags,
        edges: &[NewEdge],
        parents: &mut HashSet<Key<PatchId>>,
        children: &mut HashSet<Edge>,
        deps: &HashSet<Hash>
    ) -> Result<()> {

        for e in edges {
            debug!("add_new_edges {:?}", e);
            // If the edge has not been forgotten about,
            // insert the new version.
            let e_from = self.internal_key(&e.from, patch_id);
            let e_to = self.internal_key(&e.to, patch_id);
            let to = if flag.contains(EdgeFlags::PARENT_EDGE) { e_from } else { e_to };

            // If this is a deletion edge and not a folder edge, reconnect parents and children.
            if flag.contains(EdgeFlags::DELETED_EDGE) && !flag.contains(EdgeFlags::FOLDER_EDGE) {
                self.reconnect_parents_children(
                    branch,
                    patch_id,
                    to,
                    parents,
                    children,
                )?;
            }

            let introduced_by = self.internal_hash(&e.introduced_by, patch_id);

            if let Some(previous) = previous {
                self.delete_old_edge(branch, patch_id, previous, e_from, e_to, introduced_by)?
            }

            if flag.contains(EdgeFlags::DELETED_EDGE) && !flag.contains(EdgeFlags::FOLDER_EDGE) {
                self.delete_old_pseudo_edges(branch, patch_id, to, children, deps)?
            }

            // Let's build the edge we're about to insert.
            let e = Edge {
                flag,
                dest: e_to,
                introduced_by: patch_id.clone(),
            };

            // Finally, add the new version of the edge.
            self.put_nodes_with_rev(branch, e_from, e)?;
        }
        Ok(())
    }

    /// Add pseudo edges from all keys of `parents` to all `dest` of
    /// the edges in `children`, with the same edge flags as in
    /// `children`, plus `PSEUDO_EDGE`.
    pub fn reconnect_parents_children(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        to: Key<PatchId>,
        parents: &mut HashSet<Key<PatchId>>,
        children: &mut HashSet<Edge>,
    ) -> Result<()> {

        // Collect all the alive parents of the source of this edge.
        let mut edge = Edge::zero(EdgeFlags::PARENT_EDGE);
        parents.clear();
        parents.extend(
            self.iter_nodes(&branch, Some((to, Some(&edge))))
                .take_while(|&(k, v)| k == to && v.flag <= EdgeFlags::PARENT_EDGE | EdgeFlags::PSEUDO_EDGE)
                .filter_map(|(_, v)| if self.is_alive_or_zombie(branch, v.dest) {
                    Some(v.dest)
                } else {
                    None
                })
        );

        // Now collect all the alive children of the target of this edge.
        edge.flag = EdgeFlags::empty();
        children.clear();
        children.extend(
            self.iter_nodes(&branch, Some((to, Some(&edge))))
                .take_while(|&(k, v)| k == to && v.flag <= EdgeFlags::PSEUDO_EDGE | EdgeFlags::FOLDER_EDGE)
                .map(|(_, e)| *e)
        );

        debug!("reconnecting {:?} {:?}", parents, children);

        for &parent in parents.iter() {

            for e in children.iter() {

                // If these are not already connected
                // or pseudo-connected, add a
                // pseudo-edge.
                if !self.is_connected(branch, parent, e.dest) {

                    let pseudo_edge = Edge {
                        flag: e.flag | EdgeFlags::PSEUDO_EDGE,
                        dest: e.dest,
                        introduced_by: patch_id.clone(),
                    };
                    debug!("reconnect_parents_children: {:?} {:?}", parent, pseudo_edge);
                    self.put_nodes_with_rev(branch, parent, pseudo_edge)?;
                }
            }
        }
        Ok(())
    }

    fn delete_old_pseudo_edges(&mut self, branch: &mut Branch, patch_id: PatchId, to: Key<PatchId>, pseudo_edges: &mut HashSet<Edge>, deps: &HashSet<Hash>) -> Result<()> {
        // Now collect pseudo edges, and delete them.
        pseudo_edges.clear();
        for (_, to_edge) in self.iter_nodes(branch, Some((to, None)))
            .take_while(|&(k, v)| k == to && v.flag < EdgeFlags::DELETED_EDGE)
            .filter(|&(_, v)| v.flag.contains(EdgeFlags::PSEUDO_EDGE))
        {
            // Is this pseudo-edge a zombie marker? I.e. is there a
            // deleted edge in parallel of it? Since we haven't yet
            // introduced the new deleted edge, there is no possible
            // risk of confusion here.
            let mut e = Edge::zero(EdgeFlags::DELETED_EDGE|(to_edge.flag & (EdgeFlags::PARENT_EDGE|EdgeFlags::FOLDER_EDGE)));
            e.dest = to_edge.dest;
            let mut is_zombie_marker = to_edge.introduced_by != patch_id
                && (match self.iter_nodes(branch, Some((to, Some(&e)))).next() {
                    Some((k, v)) if k == to && v.dest == e.dest && v.flag == e.flag =>
                        v.introduced_by != patch_id,
                    _ => false
                });
            debug!("is_zombie_marker {:?}: {:?}", to_edge.dest, is_zombie_marker);
            if !is_zombie_marker {
                // This edge is not a zombie marker, we can delete it.
                pseudo_edges.insert(*to_edge);
            }
        }
        debug!("killing pseudo-edges from {:?}: {:?}", to, pseudo_edges);
        for edge in pseudo_edges.drain() {
            // Delete both directions.
            self.del_nodes_with_rev(branch, to, edge)?;
        }
        Ok(())
    }


    /// Add the new nodes (not repairing missing contexts).
    fn add_new_nodes(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        up_context: &[Key<Option<Hash>>],
        down_context: &[Key<Option<Hash>>],
        line_num: &LineId,
        flag: EdgeFlags,
        nodes: &[Vec<u8>],
    ) -> Result<()> {
        debug!("up_context {:?}", up_context);
        debug!("down_context {:?}", up_context);
        let mut v = Key {
            patch: patch_id.clone(),
            line: line_num.clone(),
        };
        let mut e = Edge {
            flag: flag ^ EdgeFlags::PARENT_EDGE,
            dest: ROOT_KEY,
            introduced_by: patch_id,
        };

        // Connect the first line to the up context.
        for c in up_context {
            e.dest = self.internal_key(c, patch_id);
            debug!("up_context: put_nodes {:?} {:?}", v, e);
            self.put_nodes_with_rev(branch, v, e)?;
        }
        debug!("up context done");

        // Insert the contents and new nodes.
        e.flag = flag;
        e.dest.patch = patch_id;

        let mut nodes = nodes.iter();
        if let Some(first_line) = nodes.next() {
            debug!("first_line = {:?}", first_line);
            let value = self.alloc_value(&first_line)?;
            debug!("put_contents {:?} {:?}", v, value);
            self.put_contents(v, value)?;
        }
        for content in nodes {
            e.dest.line = v.line + 1;
            self.put_nodes_with_rev(branch, v, e)?;

            v.line = e.dest.line;

            if !content.is_empty() {
                let value = self.alloc_value(&content)?;
                debug!("put_contents {:?} {:?}", v, value);
                self.put_contents(v, value)?;
            }
        }
        debug!("newnodes core done");

        // Connect the last new line to the down context.
        for c in down_context {
            e.dest = self.internal_key(c, patch_id);
            self.put_nodes_with_rev(branch, v, e)?;
        }
        debug!("down context done");
        Ok(())
    }
}
