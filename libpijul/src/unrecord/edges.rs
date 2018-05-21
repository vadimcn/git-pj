use rand;
use backend::*;
use patch::*;
use Result;
use super::Workspace;
use apply::find_alive::FindAlive;
use std::str::from_utf8;
use std::collections::HashSet;

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    pub(in unrecord) fn unrecord_edges(
        &mut self,
        find_alive: &mut FindAlive,
        branch: &mut Branch,
        patch_id: PatchId,
        dependencies: &HashSet<Hash>,
        previous: EdgeFlags,
        flags: EdgeFlags,
        edges: &[NewEdge],
        w: &mut Workspace,
        unused_in_other_branches: bool,
    ) -> Result<()> {
        debug!("unrecord_edges: {:?}", edges);

        // Revert the edges, i.e. add the previous edges.
        self.remove_edges(
            branch,
            patch_id,
            previous,
            flags,
            edges,
            unused_in_other_branches,
        )?;

        // If this NewEdges caused pseudo-edges to be inserted at the
        // time of applying this patch, remove them, because these
        // vertices don't need them anymore (we'll reconnect possibly
        // disconnected parts later).
        self.remove_patch_pseudo_edges(branch, patch_id, flags, edges, w)?;

        // We now take care of the connectivity of the alive graph,
        // which we must maintain.
        if previous.contains(EdgeFlags::DELETED_EDGE) {
            // This NewEdges turns a deleted edge into an alive one.
            // Therefore, unrecording this NewEdges introduced DELETED
            // edges to the graph, which might have disconnect the
            // graph. Add pseudo edges where necessary to keep the
            // alive component of the graph connected.
            let targets: Vec<_> =
                if flags.contains(EdgeFlags::PARENT_EDGE) {
                    edges.iter()
                        .map(|e| self.internal_key(&e.from, patch_id))
                        .collect()
                } else {
                    edges.iter()
                        .map(|e| self.internal_key(&e.to, patch_id))
                        .collect()
                };
            debug!("previous contains DELETED_EDGE, targets = {:?}", targets);
            self.reconnect_across_deleted_nodes(branch, dependencies, &targets)?
        } else {
            // This NewEdge turns an alive edge into a deleted
            // one. Therefore, unapplying it reintroduces alive edges,
            // but these new alive edges might have their context
            // dead. If this is the case, find their closest alive
            // ancestors and descendants, and reconnect.
            assert!(flags.contains(EdgeFlags::DELETED_EDGE));

            // If we're reintroducing a non-deleted edge, there is
            // no reason why the deleted part is still connected
            // to the alive component of the graph, so we must
            // reconnect the deleted part to its alive ancestors
            // and descendants.
            self.reconnect_deletions(branch, patch_id, edges, flags, find_alive)?

        }

        // Now, we're done reconnecting the graph. However, if this
        // NewEdges changed "folder" edges, the inodes and trees
        // tables might have to be updated.
        if flags.contains(EdgeFlags::FOLDER_EDGE) {
            if flags.contains(EdgeFlags::DELETED_EDGE) {
                // This file was deleted by this `NewEdge`. Therefore,
                // unrecording this NewEdges adds it back to the
                // repository. There are two things to do here:
                //
                // - Put it back into trees and revtrees to start
                //   following it again.
                //
                // - Since this file was *not* added by this patch
                // (because no patch can both add and delete the same
                // file), put the file back into inodes and revinodes.
                self.restore_deleted_file(branch, patch_id, edges, flags)?
            } else {
                // This file was undeleted by this patch. One way (the
                // only way?) to create such a patch is by rolling
                // back a patch that deletes a file.
                self.undo_file_reinsertion(patch_id, edges, flags)?
            }
        }

        Ok(())
    }

    /// Handles the case where the patch we are unrecording deletes an
    /// "inode" node, i.e. deletes a file from the system.
    ///
    /// We need (1) to check that, which is done in
    /// `dest_is_an_inode`, and (2) to add the file back into the
    /// `tree` and `revtree` tables (but not in the `inodes` tables).
    fn restore_deleted_file(
        &mut self,
        branch: &Branch,
        patch_id: PatchId,
        edges: &[NewEdge],
        flags: EdgeFlags,
    ) -> Result<()> {
        let is_upwards = flags.contains(EdgeFlags::PARENT_EDGE);
        for e in edges {
            let (source, dest) = if is_upwards {
                (&e.to, &e.from)
            } else {
                (&e.from, &e.to)
            };
            let source = self.internal_key(source, patch_id).to_owned();
            let dest = self.internal_key(dest, patch_id).to_owned();
            let dest_is_an_inode = if let Some(contents) = self.get_contents(dest) {
                contents.len() == 0
            } else {
                true
            };
            if dest_is_an_inode {
                // This is actually a file deletion, so it's not in
                // the tree anymore. Put it back into tree/revtrees,
                // and into inodes/revinodes.

                // Since patches *must* be recorded from top to
                // bottom, source's parent is an inode, and must be in
                // inodes/revinodes.
                let e = Edge::zero(EdgeFlags::PARENT_EDGE | EdgeFlags::FOLDER_EDGE);
                let source_parent = self.iter_nodes(branch, Some((source, Some(&e))))
                    .take_while(|&(k, _)| k == source)
                    .next()
                    .unwrap()
                    .1
                    .dest
                    .to_owned();
                debug!("source_parent = {:?}", source_parent);
                let parent_inode = if source_parent.is_root() {
                    ROOT_INODE
                } else {
                    // There is a complexity choice here: we don't
                    // want to resurrect all paths leading to this
                    // file. Resurrecting only the latest known path
                    // is not deterministic.

                    // So, if the parent doesn't exist, we attach this
                    // to the root of the repository.
                    self.get_revinodes(source_parent).unwrap_or(ROOT_INODE)
                };
                let inode = self.create_new_inode();

                let (metadata, basename) = {
                    let source_contents = self.get_contents(source).unwrap();
                    assert!(source_contents.len() >= 2);
                    let (a, b) = source_contents.as_slice().split_at(2);
                    let name = SmallString::from_str(from_utf8(b)?);
                    (FileMetadata::from_contents(a), name)
                };

                let file_id = OwnedFileId {
                    parent_inode,
                    basename,
                };
                self.put_tree(&file_id.as_file_id(), inode)?;
                self.put_revtree(inode, &file_id.as_file_id())?;

                self.replace_inodes(
                    inode,
                    FileHeader {
                        status: FileStatus::Deleted,
                        metadata,
                        key: dest,
                    },
                )?;
                self.replace_revinodes(dest, inode)?;
            }
        }
        Ok(())
    }

    fn undo_file_reinsertion(
        &mut self,
        patch_id: PatchId,
        edges: &[NewEdge],
        flags: EdgeFlags,
    ) -> Result<()> {
        for e in edges {
            let dest = if flags.contains(EdgeFlags::PARENT_EDGE) {
                &e.from
            } else {
                &e.to
            };
            let internal = self.internal_key(dest, patch_id).to_owned();
            // We're checking here that this is not a move, but
            // really the inverse of a deletion, by checking that
            // `dest` is an "inode node".
            let dest_is_an_inode = if let Some(contents) = self.get_contents(internal) {
                contents.len() == 0
            } else {
                true
            };
            if dest_is_an_inode {
                self.remove_file_from_inodes(internal)?;
            }
        }
        Ok(())
    }


    fn reconnect_deletions(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        edges: &[NewEdge],
        flags: EdgeFlags,
        find_alive: &mut FindAlive,
    ) -> Result<()> {
        // For all targets of this edges, finds its
        // alive ascendants, and add pseudo-edges.
        let is_upwards = flags.contains(EdgeFlags::PARENT_EDGE);
        let mut alive_relatives = Vec::new();
        for e in edges.iter() {
            debug!("is_upwards: {:?}", is_upwards);
            let (source, dest) = if is_upwards {
                (&e.to, &e.from)
            } else {
                (&e.from, &e.to)
            };

            let source = self.internal_key(source, patch_id);
            let dest = self.internal_key(dest, patch_id);

            if !self.is_alive(branch, dest) {
                continue;
            }

            // Collect the source's closest alive descendants, if
            // the immediate descendant is not alive.
            find_alive.clear();
            let edge = Edge::zero(EdgeFlags::DELETED_EDGE);
            for (_, dead_child) in self.iter_nodes(branch, Some((dest, Some(&edge))))
                .take_while(|&(k, v)| k == dest && v.flag == edge.flag)
            {
                find_alive.push(dead_child.dest);
            }
            debug!("find_alive {:?}", find_alive);
            alive_relatives.clear();
            let mut edge = Edge::zero(EdgeFlags::empty());
            if self.find_alive_descendants(find_alive, branch, &mut alive_relatives) {
                debug!("alive_descendants: {:?}", alive_relatives);
                for desc in alive_relatives.drain(..) {
                    if dest != desc {
                        edge.flag = EdgeFlags::PSEUDO_EDGE | (flags & EdgeFlags::FOLDER_EDGE);
                        edge.dest = desc;
                        edge.introduced_by = self.internal_hash(&e.introduced_by, patch_id);
                        debug!("put_nodes (line {:?}): {:?} {:?}", line!(), source, edge);
                        self.put_nodes_with_rev(branch, dest, edge)?;
                    }
                }
            }
            // now we'll use alive_relatives to
            // collect alive ancestors.
            debug!("source = {:?}, dest = {:?}", source, dest);
            debug!("alive_ancestors, source = {:?}", source);
            find_alive.clear();
            find_alive.push(source);
            alive_relatives.clear();
            let mut files = Vec::new();
            let mut first_file = None;
            if self.find_alive_ancestors(find_alive, branch, &mut alive_relatives, &mut first_file, &mut files) {
                debug!("alive_ancestors: {:?}", alive_relatives);
                for asc in alive_relatives.drain(..) {
                    if dest != asc {
                        edge.flag = EdgeFlags::PSEUDO_EDGE | EdgeFlags::PARENT_EDGE
                            | (flags & EdgeFlags::FOLDER_EDGE);
                        edge.dest = asc;
                        edge.introduced_by = self.internal_hash(&e.introduced_by, patch_id);
                        debug!("put_nodes (line {:?}): {:?} {:?}", line!(), dest, edge);
                        self.put_nodes_with_rev(branch, dest, edge)?;
                    }
                }
                for (mut k, mut v) in files.drain(..) {
                    assert!(v.flag.contains(EdgeFlags::DELETED_EDGE));
                    v.flag = (v.flag | EdgeFlags::PSEUDO_EDGE) ^ EdgeFlags::DELETED_EDGE;
                    self.put_nodes(branch, k, &v)?;
                }

            }
        }
        Ok(())
    }

    fn remove_edges(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        previous: EdgeFlags,
        flag: EdgeFlags,
        edges: &[NewEdge],
        unused_in_other_branches: bool,
    ) -> Result<()> {
        let mut del_edge = Edge::zero(EdgeFlags::empty());
        del_edge.introduced_by = patch_id;

        let mut edge = Edge::zero(EdgeFlags::empty());
        edge.introduced_by = patch_id;

        for e in edges {
            let int_from = self.internal_key(&e.from, patch_id);
            let int_to = self.internal_key(&e.to, patch_id);

            // Delete the edge introduced by this patch,
            // if this NewEdges is not forgetting its
            // edges.
            del_edge.flag = flag;
            del_edge.dest = int_to.clone();
            debug!("delete {:?} -> {:?}", int_from, del_edge);
            self.del_nodes_with_rev(branch, int_from, del_edge)?;

            // Add its previous version, if these edges
            // are Forget or Map (i.e. not brand new
            // edges).

            // If there are other edges with the
            // same source and target, check that
            // none of these edges knows about the
            // patch that introduced the edge we
            // want to put back in.

            edge.dest = int_to;
            debug!(
                "trying to put an edge from {:?} to {:?} back",
                int_from,
                int_to
            );
            edge.flag = previous;
            edge.introduced_by = self.internal_hash(&e.introduced_by, patch_id);

            if unused_in_other_branches {
                debug!(
                    "unused_in_other_branches: {:?} {:?} {:?}",
                    int_from,
                    edge,
                    patch_id
                );
                self.del_cemetery(int_from, &edge, patch_id)?;
            }

            // Is this edge deleted by another patch?
            // patch_id has already been removed from the table.
            let edge_is_still_absent = self.iter_cemetery(int_from, edge)
                .take_while(|&((k, v), _)| {
                    k == int_from && v.dest == edge.dest
                        && v.flag | EdgeFlags::PSEUDO_EDGE == edge.flag | EdgeFlags::PSEUDO_EDGE
                        && v.introduced_by == edge.introduced_by
                })
                .any(|(_, patch)| {
                    self.get_patch(&branch.patches, patch).is_some()
                });

            if !edge_is_still_absent {
                debug!("put_nodes (line {:?}): {:?} {:?}", line!(), int_from, edge);
                self.put_nodes_with_rev(branch, int_from, edge)?;
            }
        }

        Ok(())
    }

    fn remove_patch_pseudo_edges(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        flag: EdgeFlags,
        edges: &[NewEdge],
        w: &mut Workspace,
    ) -> Result<()> {
        if !flag.contains(EdgeFlags::DELETED_EDGE) {
            for e in edges {
                let key = if flag.contains(EdgeFlags::PARENT_EDGE) {
                    self.internal_key(&e.from, patch_id).to_owned()
                } else {
                    self.internal_key(&e.to, patch_id).to_owned()
                };

                self.remove_up_context_repair(branch, key, patch_id, &mut w.context_edges)?;

                self.remove_down_context_repair(branch, key, patch_id, &mut w.context_edges)?
            }
        }
        Ok(())
    }
}
