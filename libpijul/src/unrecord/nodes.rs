use rand;
use backend::*;
use std::mem::swap;
use std::collections::HashSet;
use Result;
use super::Workspace;

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    pub(in unrecord) fn unrecord_nodes(
        &mut self,
        branch: &mut Branch,
        patch_id: PatchId,
        dependencies: &HashSet<Hash>,
        up_context: &[Key<Option<Hash>>],
        down_context: &[Key<Option<Hash>>],
        line_num: LineId,
        flag: EdgeFlags,
        nodes: &[Vec<u8>],
        w: &mut Workspace,
        unused_in_other_branch: bool,
    ) -> Result<()> {
        debug!(
            "unrecord_nodes: {:?} {:?} {:?}",
            patch_id,
            line_num,
            nodes.len()
        );
        // Delete the new nodes.


        // Start by deleting all the "missing context repair" we've
        // added when applying this patch, i.e. all the extra
        // pseudo-edges that were inserted to connect the alive set of
        // vertices.

        // We make the assumption that no pseudo-edge is a shortcut
        // for this NewNodes. This is because `nodes` is nonempty:
        // indeed, any such pseudo-edge would stop at one of the nodes
        // introduced by this NewNodes.
        assert!(nodes.len() != 0);

        // Remove the zombie edges introduced to repair the context,
        // if it was missing when we applied this NewNodes.
        for c in up_context.iter() {
            let c = self.internal_key(c, patch_id);
            self.remove_up_context_repair(branch, c, patch_id, &mut w.context_edges)?;
        }
        for c in down_context.iter() {
            let c = self.internal_key(c, patch_id);
            self.remove_down_context_repair(branch, c, patch_id, &mut w.context_edges)?;
        }

        // Delete the nodes and all their adjacent edges.
        let mut k = Key {
            patch: patch_id.clone(),
            line: line_num.clone(),
        };
        for i in 0..nodes.len() {
            debug!("starting k: {:?}", k);
            if unused_in_other_branch {
                // If this patch is unknown to any other branch,
                // delete the contents of this node.
                self.del_contents(k, None)?;
            }

            // Delete all edges adjacent to this node, which will also
            // delete the node (we're only storing edges).
            loop {
                // Find the next edge from this key, or break if we're done.
                let mut edge = if let Some(edge) = self.get_nodes(branch, k, None) {
                    edge.to_owned()
                } else {
                    break;
                };

                debug!("{:?} {:?}", k, edge);
                // Kill that edge in both directions.
                self.del_nodes(branch, k, Some(&edge))?;
                edge.flag.toggle(EdgeFlags::PARENT_EDGE);
                swap(&mut edge.dest, &mut k);
                self.del_nodes(branch, k, Some(&edge))?;
                swap(&mut edge.dest, &mut k);
            }

            // If this is a file addition, delete it from inodes/revinodes.
            if flag.contains(EdgeFlags::FOLDER_EDGE) {
                self.undo_file_addition(patch_id, k, i, nodes, down_context)?
            }

            // Increment the line id (its type, LineId, implements
            // little-endian additions with usize. See the `backend`
            // module).
            k.line += 1
        }

        // From all nodes in the down context, climb deleted paths up
        // until finding alive ancestors, and add pseudo-edges from
        // these ancestors to the down context.
        let internal_down_context: Vec<_> = down_context
            .iter()
            .map(|c| self.internal_key(c, patch_id))
            .collect();
        if flag.contains(EdgeFlags::FOLDER_EDGE) {
            // unimplemented!()
        } else {
            self.reconnect_across_deleted_nodes(branch, dependencies, &internal_down_context)?
        }
        Ok(())
    }

    fn undo_file_addition(
        &mut self,
        patch_id: PatchId,
        k: Key<PatchId>,
        i: usize,
        nodes: &[Vec<u8>],
        down_context: &[Key<Option<Hash>>],
    ) -> Result<()> {
        debug!(
            "remove_file_from_inodes: {:?} {:?} {:?}",
            k,
            i,
            nodes.len() - 1
        );
        // Is this a move?
        if i == nodes.len() - 1 && !down_context.is_empty() {
            // Yes, this is a move.
            assert_eq!(down_context.len(), 1);
            let internal = self.internal_key(&down_context[0], patch_id).to_owned();
            // Mark the file as moved.
            let inode = self.get_revinodes(internal).unwrap().to_owned();
            let mut file = self.get_inodes(inode).unwrap().to_owned();
            file.status = FileStatus::Moved;
            self.replace_inodes(inode, file)?;
        } else {
            // No, this is actually an addition. We have to leave the
            // inodes in place in trees and revtrees, and remove them
            // from inodes.
            self.remove_file_from_inodes(k)?;
        }
        Ok(())
    }
}
