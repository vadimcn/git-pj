use backend::*;
use std::collections::HashSet;

#[derive(Debug)]
pub struct FindAlive {
    stack: Vec<Key<PatchId>>,
    visited: HashSet<Key<PatchId>>,
}

impl FindAlive {
    pub fn new() -> Self {
        FindAlive {
            stack: Vec::new(),
            visited: HashSet::new()
        }
    }
    pub fn clear(&mut self) {
        self.stack.clear();
        self.visited.clear();
    }
    pub fn push(&mut self, k: Key<PatchId>) {
        self.stack.push(k)
    }
    pub fn pop(&mut self) -> Option<Key<PatchId>> {
        while let Some(p) = self.stack.pop() {
            if !self.visited.contains(&p) {
                self.visited.insert(p.clone());
                return Some(p)
            }
        }
        None
    }
}

impl<U: Transaction, R> GenericTxn<U, R> {

    /// Recursively find all ancestors by doing a DFS, and collect all
    /// edges until finding an alive ancestor.
    ///
    /// Returns whether or not at least one traversed vertex was dead
    /// (or otherwise said, returns `false` if and only if there all
    /// vertices in `find_alive` are alive).
    pub fn find_alive_ancestors(&self,
                                find_alive: &mut FindAlive,
                                branch: &Branch,
                                alive: &mut Vec<Key<PatchId>>,
                                file_ancestor: &mut Option<Key<PatchId>>,
                                files: &mut Vec<(Key<PatchId>, Edge)>) -> bool {
        let mut first_is_alive = false;
        let mut file = None;
        while let Some(a) = find_alive.pop() {
            if self.is_alive(branch, a) {
                // This node is alive.
                alive.push(a);
            } else {
                first_is_alive = true;
                let e = Edge::zero(EdgeFlags::PARENT_EDGE|EdgeFlags::DELETED_EDGE);
                for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                    .take_while(|&(k, _)| k == a) {

                        debug!("find_alive_ancestors: {:?}", v);
                        if v.flag.contains(EdgeFlags::FOLDER_EDGE) {
                            // deleted file.
                            file = Some(a);
                            *file_ancestor = Some(a)
                        } else {
                            find_alive.push(v.dest)
                        }
                    }
            }
        }
        debug!("file {:?}", file);
        if let Some(file) = file {
            find_alive.clear();
            find_alive.push(file);
            while let Some(a) = find_alive.pop() {
                debug!("file {:?}", a);
                if !self.is_alive(branch, a) {
                    debug!("not alive");
                    first_is_alive = true;
                    let e = Edge::zero(EdgeFlags::PARENT_EDGE|EdgeFlags::DELETED_EDGE|EdgeFlags::FOLDER_EDGE);
                    for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                        .take_while(|&(k, _)| k == a) {

                            debug!("file find_alive_ancestors: {:?}", v);
                            // deleted file, collect.
                            files.push((a, *v));
                            find_alive.push(v.dest)
                        }
                }
            }
        }

        first_is_alive
    }

    /// Recursively find all descendants by doing a DFS on deleted
    /// edges (including folder edges), and collect all edges until
    /// finding an alive or zombie descendant.
    ///
    /// Returns whether or not at least one traversed vertex was dead
    /// (or otherwise said, returns `false` if and only if there all
    /// vertices in `find_alive` are alive).
    pub fn find_alive_descendants(&self,
                                  find_alive: &mut FindAlive,
                                  branch: &Branch,
                                  alive: &mut Vec<Key<PatchId>>) -> bool {
        let mut first_is_alive = false;
        debug!("begin find_alive_descendants");
        while let Some(a) = find_alive.pop() {
            debug!("find_alive_descendants, a = {:?}", a);
            if self.is_alive(branch, a) {
                debug!("alive: {:?}", a);
                alive.push(a);
            } else {
                // Else, we need to explore its deleted descendants.
                first_is_alive = true;
                let e = Edge::zero(EdgeFlags::empty());
                for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                    .take_while(|&(k, _)| k == a)
                    .filter(|&(_, v)| !v.flag.contains(EdgeFlags::PARENT_EDGE))
                {
                    debug!("v = {:?}", v);
                    if v.flag.contains(EdgeFlags::DELETED_EDGE) {
                        debug!("find_alive_descendants: {:?}", v);
                        find_alive.push(v.dest)
                    } else {
                        debug!("alive in for: {:?}", v.dest);
                        alive.push(v.dest)
                    }
                }
            }
        }
        debug!("end find_alive_descendants");
        first_is_alive
    }

    fn find_alive(&self,
                  branch: &Branch,
                  find_alive: &mut FindAlive,
                  alive: &mut HashSet<Key<PatchId>>,
                  file: &mut Option<Key<PatchId>>,
                  current: Key<PatchId>,
                  flag: EdgeFlags) {
        find_alive.clear();
        debug!("find_alive: {:?}", current);
        find_alive.push(current);
        while let Some(current) = find_alive.pop() {
            debug!("find_alive, current = {:?}", current);
            if self.is_alive(branch, current) {
                alive.insert(current.clone());
            } else {
                let e = Edge::zero(flag);
                for (_, e) in self.iter_nodes(branch, Some((current, Some(&e))))
                    .take_while(|&(k, v)| k == current && v.flag|EdgeFlags::FOLDER_EDGE|EdgeFlags::PSEUDO_EDGE == flag|EdgeFlags::FOLDER_EDGE|EdgeFlags::PSEUDO_EDGE) {
                        debug!("e = {:?}", e);
                        // e might be FOLDER_EDGE here.
                        if e.flag.contains(EdgeFlags::FOLDER_EDGE) && file.is_none() {
                            *file = Some(current.clone())
                        } else {
                            find_alive.push(e.dest.clone())
                        }
                    }
            }
        }
    }

    /// Find the alive descendants of `current`. `cache` is here to
    /// avoid cycles, and `alive` is an accumulator of the
    /// result. Since this search stops at files, if the file
    /// containing these lines is ever hit, it will be put in `file`.
    pub fn find_alive_nonfolder_descendants(&self,
                                            branch: &Branch,
                                            find_alive: &mut FindAlive,
                                            alive: &mut HashSet<Key<PatchId>>,
                                            file: &mut Option<Key<PatchId>>,
                                            current: Key<PatchId>) {

        self.find_alive(branch, find_alive, alive, file, current, EdgeFlags::DELETED_EDGE)
    }

    /// Find the alive ancestors of `current`. `cache` is here to
    /// avoid cycles, and `alive` is an accumulator of the
    /// result. Since this search stops at files, if the file
    /// containing these lines is ever hit, it will be put in `file`.
    pub fn find_alive_nonfolder_ancestors(&self,
                                          branch: &Branch,
                                          find_alive: &mut FindAlive,
                                          alive: &mut HashSet<Key<PatchId>>,
                                          file: &mut Option<Key<PatchId>>,
                                          current: Key<PatchId>) {

        self.find_alive(branch, find_alive, alive, file, current, EdgeFlags::DELETED_EDGE|EdgeFlags::PARENT_EDGE)
    }
}
