use rand;
use backend::*;
use std::mem::swap;
use std::collections::HashMap;
use Result;

impl<'env, T: rand::Rng> MutTxn<'env, T> {

    fn collect_up_context_repair(&self,
                                 branch: &Branch,
                                 key: Key<PatchId>,
                                 patch_id: PatchId,
                                 edges: &mut HashMap<Key<PatchId>, Edge>) {

        debug!("collect up {:?}", key);
        let edge = Edge::zero(EdgeFlags::PARENT_EDGE | EdgeFlags::PSEUDO_EDGE);
        for (k, v) in self.iter_nodes(branch, Some((key, Some(&edge))))
            .take_while(|&(k, v)| {
                k == key && v.flag <= EdgeFlags::PARENT_EDGE | EdgeFlags::PSEUDO_EDGE | EdgeFlags::FOLDER_EDGE &&
                    v.introduced_by == patch_id
            }) {

                if !edges.contains_key(&k) {
                    edges.insert(k.to_owned(), v.to_owned());

                    self.collect_up_context_repair(branch, v.dest, patch_id, edges)
                }
            }

    }

    fn collect_down_context_repair(&self,
                                   branch: &Branch,
                                   key: Key<PatchId>,
                                   patch_id: PatchId,
                                   edges: &mut HashMap<Key<PatchId>, Edge>) {

        debug!("collect down {:?}", key);

        let edge = Edge::zero(EdgeFlags::PSEUDO_EDGE);
        for (k, v) in self.iter_nodes(branch, Some((key, Some(&edge))))
            .take_while(|&(k, v)| {
                k == key && v.flag <= EdgeFlags::PSEUDO_EDGE | EdgeFlags::FOLDER_EDGE && v.introduced_by == patch_id
            }) {

                if !edges.contains_key(&k) {
                    edges.insert(k.to_owned(), v.to_owned());

                    self.collect_down_context_repair(branch, v.dest, patch_id, edges)
                }
        }

    }

    pub fn remove_up_context_repair(&mut self,
                                    branch: &mut Branch,
                                    key: Key<PatchId>,
                                    patch_id: PatchId,
                                    edges: &mut HashMap<Key<PatchId>, Edge>)
                                    -> Result<()> {

        self.collect_up_context_repair(branch, key, patch_id, edges);
        for (mut k, mut v) in edges.drain() {

            debug!("remove {:?} {:?}", k, v);

            self.del_nodes(branch, k, Some(&v))?;
            swap(&mut k, &mut v.dest);
            v.flag.toggle(EdgeFlags::PARENT_EDGE);
            self.del_nodes(branch, k, Some(&v))?;
        }

        Ok(())
    }

    pub fn remove_down_context_repair(&mut self,
                                      branch: &mut Branch,
                                      key: Key<PatchId>,
                                      patch_id: PatchId,
                                      edges: &mut HashMap<Key<PatchId>, Edge>)
                                      -> Result<()> {

        self.collect_down_context_repair(branch, key, patch_id, edges);
        for (mut k, mut v) in edges.drain() {
            self.del_nodes(branch, k, Some(&v))?;
            swap(&mut k, &mut v.dest);
            v.flag.toggle(EdgeFlags::PARENT_EDGE);
            self.del_nodes(branch, k, Some(&v))?;
        }

        Ok(())
    }
}
