use backend::*;
use super::*;

impl<A: Transaction, R: rand::Rng> GenericTxn<A, R> {
    fn add_lines_up_context(&self, line_index: usize, diff: &Diff<A>) -> Vec<Key<Option<PatchId>>> {
        if diff.lines_a[line_index].is_root() {

            let mut up_context = Vec::new();
            let status = *diff.status.get(&line_index).unwrap();
            if status < Status::End {
                // If the up context is the beginning of a conflict,
                // or a separator, the actual up context is the last
                // line before the conflict (this conflict might be
                // nested, which is why we need a loop to get up to
                // that line).
                let i = diff.conflicts_ancestors.get(&line_index).unwrap();

                // Now, maybe that line was inserted by the patch
                // we're preparing.
                if let Some(&(line, _)) = diff.conflicts_down_contexts.get(i) {
                    // If this is case, use that line as the up context.
                    up_context.push(Key { patch: None, line })
                } else {
                    // Else, the up context is the first non-marker
                    // line in `diff.lines_a` before `i`. What if it's
                    // been deleted?
                    let mut i = *i;
                    while i > 0 && diff.lines_a[i].is_root() {
                        i -= 1
                    }
                    let up = diff.lines_a[i].to_owned();
                    up_context.push(Key {
                        patch: Some(up.patch),
                        line: up.line,
                    })
                }
            } else {
                // End of a conflict.

                let i0 = *diff.conflicts_ancestors.get(&line_index).unwrap();
                // In case we're in an embedded conflict, get up to the
                // line before the first conflict marker.
                let mut i = line_index;
                debug!("add_lines_up_context, i0 = {:?}", i0);

                while i > i0 {
                    debug!(
                        "i = {:?} a[i] = {:?}, a[i-1] = {:?}",
                        i,
                        diff.lines_a[i],
                        diff.lines_a[i - 1]
                    );
                    if diff.lines_a[i].is_root() && !diff.lines_a[i - 1].is_root() {
                        // If we're still at a conflict marker
                        let up = diff.lines_a[i - 1].to_owned();
                        up_context.push(Key {
                            patch: Some(up.patch),
                            line: up.line,
                        })
                    }
                    if let Some(&(line, _)) = diff.conflicts_down_contexts.get(&i) {
                        up_context.push(Key { patch: None, line })
                    }
                    i -= 1
                }
            }
            up_context
        } else {
            // Here, since additions can only "pause" if there is a
            // conflict, and this is not a conflict, there's no need
            // to check for down contexts.
            //
            // Note: maybe some future diff algorithms might require
            // this case to check `diff.conflicts_down_contexts`.
            let up_context = diff.lines_a[line_index];
            vec![
                Key {
                    patch: Some(up_context.patch),
                    line: up_context.line,
                },
            ]
        }
    }

    fn add_lines_down_context_trailing_equals(
        &self,
        line_index: usize,
        line_id: LineId,
        diff: &mut Diff<A>,
        trailing_equals: usize,
    ) -> Rc<RefCell<Vec<Key<Option<PatchId>>>>> {

        let status = *diff.status.get(&line_index).unwrap();
        let ancestor = *diff.conflicts_ancestors.get(&line_index).unwrap();
        match status {
            Status::Begin if line_index >= diff.lines_a.len() - trailing_equals => {
                // If the first line of the "trailing equals" is a
                // conflict marker, we already know that nothing
                // below will change. The down context lines can
                // already be produced, and moreover will not be
                // produced later on, as the algorithm stops here.
                //
                // down_context = each side of the conflict. We
                // need the `ConflictSidesIter` iterator because
                // this conflict might be nested.
                Rc::new(RefCell::new(
                    ConflictSidesIter {
                        diff,
                        started: false,
                        current: ancestor,
                        level: 0,
                    }.filter_map(|side| {
                        let k = diff.lines_a[side + 1];
                        if k.is_root() {
                            None
                        } else {
                            Some(Key {
                                patch: Some(k.patch),
                                line: k.line,
                            })
                        }
                    })
                        .collect(),
                ))
            }
            _ => {
                // Here, we're inserting a line just before a conflict
                // marker or separator, where the end conflict marker
                // (which might or might not be the current line) is
                // in the trailing equals part of the file.
                //
                // Therefore, nothing will happen after this function
                // returns, and this is our last chance to add the
                // down context for this addition.
                let mut descendant = *diff.conflicts_descendants.get(&ancestor).unwrap();
                let down = if descendant >= diff.lines_a.len() - trailing_equals {
                    // down_context = next line after the conflict
                    if descendant + 1 < diff.lines_a.len() {
                        if diff.lines_a[descendant + 1].is_root() {
                            // If the next line after the conflict is
                            // itself a new conflict.
                            Rc::new(RefCell::new(
                                ConflictSidesIter {
                                    diff,
                                    started: false,
                                    current: descendant + 1,
                                    level: 0,
                                }.filter_map(|side| {
                                    let k = diff.lines_a[side + 1];
                                    if k.is_root() {
                                        None
                                    } else {
                                        Some(Key {
                                            patch: Some(k.patch),
                                            line: k.line,
                                        })
                                    }
                                })
                                    .collect(),
                            ))
                        } else {
                            // If there is a line after the conflict, but
                            // it is not a new conflict.
                            let k = diff.lines_a[descendant+1];
                            Rc::new(RefCell::new(vec![
                                Key {
                                    patch: Some(k.patch),
                                    line: k.line,
                                },
                            ]))
                        }
                    } else {
                        // If there is no line after the conflict.
                        Rc::new(RefCell::new(Vec::new()))
                    }
                } else {
                    Rc::new(RefCell::new(Vec::new()))
                };
                debug!("inserting down contexts {:?} {:?}", line_index, line_id);
                diff.conflicts_down_contexts.insert(line_index, (
                    line_id,
                    down.clone(),
                ));
                down
            }
        }
    }

    fn add_lines_down_context(
        &self,
        line_index: usize,
        line_id: LineId,
        diff: &mut Diff<A>,
        trailing_equals: usize,
    ) -> Rc<RefCell<Vec<Key<Option<PatchId>>>>> {
        if diff.lines_a[line_index].is_root() {
            debug!(
                "line_index {:?}, len {:?}, trailing_equals {:?}",
                line_index,
                diff.lines_a.len(),
                trailing_equals
            );
            self.add_lines_down_context_trailing_equals(line_index, line_id, diff, trailing_equals)
        } else {
            let down_context = diff.lines_a[line_index];
            Rc::new(RefCell::new(vec![
                Key {
                    patch: Some(down_context.patch),
                    line: down_context.line.clone(),
                },
            ]))
        }
    }

    pub(crate) fn add_lines(
        &self,
        up_line_index: usize,
        down_line_index: Option<usize>,
        diff: &mut Diff<A>,
        lines: &[&[u8]],
        trailing_equals: usize,
    ) -> patch::Change<Rc<RefCell<ChangeContext<PatchId>>>> {
        debug!("add_lines: {:?}", lines);
        let up_context = self.add_lines_up_context(up_line_index, diff);
        let down_context = if let Some(down) = down_line_index {
            self.add_lines_down_context(
                down,
                *diff.line_num + (lines.len() - 1),
                diff,
                trailing_equals,
            )
        } else {
            Rc::new(RefCell::new(Vec::new()))
        };
        debug!("adding lines {}", lines.len());
        let changes = Change::NewNodes {
            up_context: Rc::new(RefCell::new(up_context)),
            down_context,
            line_num: diff.line_num.clone(),
            flag: EdgeFlags::empty(),
            nodes: lines.iter().map(|x| x.to_vec()).collect(),
            inode: diff.inode.clone(),
        };
        *diff.line_num += lines.len();
        changes
    }
}

/// Iterator over the first lines of sides of a conflict. This is
/// non-trivial because conflicts can be nested. In such a case, this
/// iterator returns the first lines of all sides of nested conflicts.
struct ConflictSidesIter<'c, 'a: 'c, 'b: 'c, T: 'a> {
    level: usize,
    started: bool,
    current: usize,
    diff: &'c Diff<'a, 'b, T>,
}

impl<'c, 'a: 'c, 'b: 'c, T: 'a> Iterator for ConflictSidesIter<'c, 'a, 'b, T> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if (self.level == 0 && self.started) || self.current >= self.diff.lines_a.len() {
                return None;
            } else {
                self.started = true;
                if self.diff.lines_a[self.current].is_root() {
                    let status = *self.diff.status.get(&self.current).unwrap();
                    if status == Status::Begin {
                        self.level += 1
                    }
                    self.current += 1;
                    if status == Status::End {
                        self.level -= 1;
                    } else {
                        return Some(self.current - 1);
                    }
                } else {
                    self.current += 1;
                }
            }
        }
    }
}
