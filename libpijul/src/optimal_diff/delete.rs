use backend::*;
use super::*;

impl<A: Transaction, R: rand::Rng> GenericTxn<A, R> {
   fn delete_edges(&self,
                    branch: &Branch,
                    edges: &mut Vec<patch::NewEdge>,
                    key: Option<Key<PatchId>>,
                    flag: EdgeFlags) {

        debug!("deleting edges");
        match key {
            Some(key) => {
                debug!("key: {:?}", key);
                debug_assert!(
                    self.get_patch(&branch.patches, key.patch).is_some()
                        || key.is_root()
                );
                let ext_hash = self.external_hash(key.patch);
                let edge = Edge::zero(flag);
                // For all alive edges from `key` with flag `flag`
                // (remember that `flag` contains `PARENT_EDGE`).
                for (k, v) in self.iter_nodes(&branch, Some((key, Some(&edge))))
                    .take_while(|&(k, v)| k == key && v.flag <= edge.flag|EdgeFlags::PSEUDO_EDGE|EdgeFlags::EPSILON_EDGE)
                {
                    if v.flag.contains(EdgeFlags::PSEUDO_EDGE) {
                        let mut edge = v.clone();
                        edge.flag = EdgeFlags::DELETED_EDGE|EdgeFlags::PARENT_EDGE;
                        edge.introduced_by = ROOT_PATCH_ID;
                        let mut edges = self.iter_nodes(&branch, Some((key, Some(&edge))));
                        match edges.next() {
                            Some((k_, v_)) if k_ == key && v_.flag == edge.flag => {
                                // `key` has at least one deleted edge
                                // pointing to it, so key is either
                                // dead or a zombie. We need to
                                // confirm that it is actually deleted
                                // by deleting edge `v`.
                                debug!("zombie edge: {:?} {:?}", k, v);
                            }
                            x => {
                                // The target of this edge is not deleted,
                                // and not a zombie vertex.  We can ignore
                                // this pseudo-edge, as deleting all alive
                                // edges to the target will have the same
                                // effect.
                                debug!("not a zombie: {:?}", x);
                                continue
                            }
                        }
                    };
                    debug!("delete: {:?} {:?}", k, v);
                    debug!("actually deleting");
                    edges.push(patch::NewEdge {
                        from: Key {
                            patch: Some(ext_hash.to_owned()),
                            line: key.line.clone(),
                        },
                        to: Key {
                            patch: Some(self.external_hash(v.dest.patch).to_owned()),
                            line: v.dest.line.clone(),
                        },
                        introduced_by: Some(self.external_hash(v.introduced_by).to_owned()),
                    });
                }
            }
            None => {}
        }
    }

    // i0: index of the first deleted line.
    // i > i0: index of the first non-deleted line (might or might not exist).
    pub(in optimal_diff) fn delete_lines(&self, branch: &Branch, diff: &mut Diff<A>, i0: usize, i1: usize) -> Deletion {
        debug!("delete_lines: {:?}", i1 - i0);
        let mut edges = Vec::with_capacity(i1 - i0);
        let mut contains_conflict = None;
        for i in i0..i1 {
            debug!("deleting line {:?}", diff.lines_a[i]);
            if diff.lines_a[i] == ROOT_KEY {
                // We've deleted a conflict marker.
                contains_conflict = diff.conflicts_ancestors.get(&i)
            } else {
                self.delete_edges(branch, &mut edges, Some(diff.lines_a[i]), EdgeFlags::PARENT_EDGE);
            }
        }

        let mut conflict_ordering = Vec::new();

        // If this is an ordering conflict, add the relevant edges.
        if contains_conflict.is_some() {
            if i0 > 0 && i1 < diff.lines_a.len() {
                let from_patch = diff.lines_a[i0 - 1].patch; // self.external_hash(diff.lines_a[i0 - 1].patch);
                let to_patch = diff.lines_a[i1].patch; // self.external_hash(diff.lines_a[i1].patch);
                // TODO: check that these two lines are not already linked.
                if !self.is_connected(branch, diff.lines_a[i0-1], diff.lines_a[i1]) {
                    debug!("conflict ordering between {:?} and {:?}", i0-1, i1);
                    conflict_ordering.push(Change::NewNodes {
                        up_context: Rc::new(RefCell::new(vec![Key {
                            patch: Some(from_patch.to_owned()),
                            line: diff.lines_a[i0-1].line.clone(),
                        }])),
                        down_context: Rc::new(RefCell::new(vec![Key {
                            patch: Some(to_patch.to_owned()),
                            line: diff.lines_a[i1].line.clone(),
                        }])),
                        line_num: diff.line_num.clone(),
                        flag: EdgeFlags::EPSILON_EDGE,
                        nodes: vec![Vec::new()],
                        inode: diff.inode.clone(),
                    });
                    *diff.line_num += 1
                }
            }
        }

        // Deletion
        Deletion {
            del: if edges.len() > 0 {
                Some(Change::NewEdges {
                    edges: edges,
                    previous: EdgeFlags::PARENT_EDGE,
                    flag: EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE,
                    inode: diff.inode.clone(),
                })
            } else {
                None
            },
            conflict_ordering,
        }
    }

    /// Remove the deleted edges of a zombie.
    pub(in optimal_diff) fn confirm_zombie(&self, branch: &Branch, diff: &Diff<A>, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>, key: Key<PatchId>) {
        debug!("confirm_zombie: {:?}", key);
        let mut zombie_edges = Vec::new();
        self.delete_edges(branch, &mut zombie_edges, Some(key), EdgeFlags::DELETED_EDGE|EdgeFlags::PARENT_EDGE);
        if !zombie_edges.is_empty() {
            actions.push(Record::Change {
                change: Change::NewEdges {
                    edges: zombie_edges,
                    previous: EdgeFlags::PARENT_EDGE|EdgeFlags::DELETED_EDGE,
                    flag: EdgeFlags::PARENT_EDGE,
                    inode: diff.inode.clone(),
                },
                file: diff.file.clone(),
                conflict_reordering: Vec::new(),
            })
        }
    }
}
