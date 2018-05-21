use patch;
use graph;
use patch::{Change, Record, ChangeContext};
use {GenericTxn, Result};
use backend::*;
use graph::Graph;

use std;
use sanakirja::value::Value;
use rand;
use std::path::PathBuf;
use std::rc::Rc;
use conflict;
use std::collections::HashMap;
use std::cell::RefCell;

mod delete;
mod add;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Status {
    Begin,
    Separator,
    End,
}

#[doc(hidden)]
pub(crate) struct Diff<'a, 'b, T: 'a> {
    lines_a: Vec<Key<PatchId>>,
    contents_a: Vec<Value<'a, T>>,
    conflicts_ancestors: HashMap<usize, usize>,
    conflicts_descendants: HashMap<usize, usize>,
    conflicts_sides: HashMap<usize, Vec<usize>>,
    conflicts_down_contexts: HashMap<usize, (LineId, Rc<RefCell<Vec<Key<Option<PatchId>>>>>)>,
    current_conflict_ancestor: Vec<usize>,
    status: HashMap<usize, Status>,
    inode: Key<Option<Hash>>,
    file: Rc<PathBuf>,
    line_num: &'b mut LineId,
}

#[derive(Debug)]
pub(crate) struct Cursors {
    i: usize,
    j: usize,
    oi: Option<usize>,
    oj: Option<usize>,
    pending: Pending,
    last_alive_context: usize,
    leading_equals: usize,
    trailing_equals: usize,
}

impl<'a, 'b, T: Transaction + 'a> graph::LineBuffer<'a, T> for Diff<'a, 'b, T> {
    fn output_line(&mut self, k: &Key<PatchId>, c: Value<'a, T>) -> Result<()> {
        self.lines_a.push(k.clone());
        self.contents_a.push(c);
        Ok(())
    }

    fn begin_conflict(&mut self) -> Result<()> {
        let len = self.lines_a.len();
        self.current_conflict_ancestor.push(len);
        self.status.insert(len, Status::Begin);

        self.output_conflict_marker(conflict::START_MARKER)
    }

    fn end_conflict(&mut self) -> Result<()> {
        let len = self.lines_a.len();
        self.status.insert(len, Status::End);
        self.conflicts_descendants.insert(*self.current_conflict_ancestor.last().unwrap(), len);

        self.output_conflict_marker(conflict::END_MARKER)?;

        self.current_conflict_ancestor.pop();
        Ok(())
    }

    fn conflict_next(&mut self) -> Result<()> {
        self.status.insert(self.lines_a.len(), Status::Separator);
        {
            let e = self.conflicts_sides.entry(*self.current_conflict_ancestor.last().unwrap()).or_insert(Vec::new());
            e.push(self.lines_a.len());
        }
        self.output_conflict_marker(conflict::SEPARATOR)
    }

    fn output_conflict_marker(&mut self, marker: &'a str) -> Result<()> {
        let l = self.lines_a.len();
        self.lines_a.push(ROOT_KEY.clone());
        self.contents_a.push(Value::from_slice(marker.as_bytes()));
        self.conflicts_ancestors.insert(l, *self.current_conflict_ancestor.last().unwrap());
        Ok(())
    }
}

#[derive(Debug)]
struct Deletion {
    del: Option<Change<Rc<RefCell<ChangeContext<PatchId>>>>>,
    conflict_ordering: Vec<Change<Rc<RefCell<ChangeContext<PatchId>>>>>
}

#[derive(Debug)]
enum Pending {
    None,
    Deletion(Deletion),
    Addition(Change<Rc<RefCell<ChangeContext<PatchId>>>>)
}

impl Pending {
    fn take(&mut self) -> Pending {
        std::mem::replace(self, Pending::None)
    }
    fn is_none(&self) -> bool {
        if let Pending::None = *self { true } else { false }
    }
}

use std::ops::{Index, IndexMut};
struct Matrix<T> {
    rows: usize,
    cols: usize,
    v: Vec<T>,
}
impl<T: Clone> Matrix<T> {
    fn new(rows: usize, cols: usize, t: T) -> Self {
        Matrix {
            rows: rows,
            cols: cols,
            v: vec![t; rows * cols],
        }
    }
}
impl<T> Index<usize> for Matrix<T> {
    type Output = [T];
    fn index(&self, i: usize) -> &[T] {
        &self.v[i * self.cols..(i + 1) * self.cols]
    }
}
impl<T> IndexMut<usize> for Matrix<T> {
    fn index_mut(&mut self, i: usize) -> &mut [T] {
        &mut self.v[i * self.cols..(i + 1) * self.cols]
    }
}

fn amend_down_context<A:Transaction>(diff: &Diff<A>, i0: usize, i: usize) {
    if let Some(&(ref line, ref down)) = diff.conflicts_down_contexts.get(&i0) {
        // `line` is the LineId of the line inserted by
        // this patch before i0 (remember that i0 is a
        // conflict marker).
        debug!("lines_eq, line = {:?}", line);
        let key = diff.lines_a[i];
        down.borrow_mut().push(Key {
            patch: Some(key.patch),
            line: key.line
        })
    }
}

impl<A: Transaction, R: rand::Rng> GenericTxn<A, R> {

    fn lines_eq(&self, branch: &Branch, diff: &mut Diff<A>, b: &[&[u8]], cursors: &mut Cursors, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>) {
        // Two lines are equal. If we were collecting lines to add or
        // delete, we must stop here (in order to get the smallest
        // possible patch).
        debug!("eq: {:?} {:?}", cursors.i, cursors.j);

        // Here we know that diff.lines_a[cursors.i] is equal to
        // b[cursors.j].
        //
        // If the line just before this equality was a conflict marker
        // (i.e. an index i such that diff.lines_a[i].is_root()), some
        // other conflict markers might miss edges (because of how we
        // record that situation in
        // optimal_diff::add::add_lines_down_context). If this is the
        // case, we need to add those missing edges.

        if cursors.i > 0 && diff.lines_a[cursors.i - 1].is_root() {
            // i0 is the conflict's ancestor (another "root" line).
            let status = *diff.status.get(&(cursors.i-1)).unwrap();
            let mut i0 = *diff.conflicts_ancestors.get(&(cursors.i-1)).unwrap();
            if status < Status::End {
                debug!("lines_eq, i0 = {:?}", i0);
                // This is an equality immediately after a conflict
                // marker. If there was a line added just before the
                // beginning of the conflict, add an edge. Else, don't
                // do anything.
                amend_down_context(diff, i0, cursors.i)
            } else {
                // End of the conflict. There might have been
                // additions just before a conflict marker (separator
                // or end) of this conflict. We need to add all of
                // them.
                i0 += 1;
                while i0 < cursors.i {
                    amend_down_context(diff, i0, cursors.i);
                    i0 += 1
                }
            }
        }

        //
        if let Some(i0) = cursors.oi.take() {
            // If we were collecting lines to delete (from i0, inclusive).
            let i0 = cursors.leading_equals + i0;
            let i = cursors.leading_equals + cursors.i;
            debug!("deleting from {} to {} / {}", i0, i, diff.lines_a.len());
            if i0 < i {
                let dels = self.delete_lines(branch, diff, i0, i);
                stop_del(diff.file.clone(), cursors, dels, actions)
            }
        } else if let Some(j0) = cursors.oj.take() {
            // Else, if we were collecting lines to add (from j0, inclusive).
            let j0 = cursors.leading_equals + j0;
            let j = cursors.leading_equals + cursors.j;
            let i = cursors.leading_equals + cursors.i;
            debug!("adding from {} to {} / {}, context {}",
                   j0, cursors.j, b.len(), cursors.last_alive_context);
            if j0 < j {
                let adds = self.add_lines(cursors.last_alive_context,
                                          Some(i),
                                          diff,
                                          &b[j0..j],
                                          cursors.trailing_equals);
                stop_add(diff.file.clone(), cursors, adds, actions)
            }
        }
        // "Confirm" line i / j, if it is a zombie line.
        self.confirm_zombie(branch, diff, actions, diff.lines_a[cursors.leading_equals + cursors.i]);

        // Move on to the next step.
        cursors.last_alive_context = cursors.leading_equals + cursors.i;
        cursors.i += 1;
        cursors.j += 1;

    }

    fn move_i(&self, diff: &mut Diff<A>, b: &[&[u8]], cursors: &mut Cursors) {
        // We will delete things starting from i (included).
        // If we are currently adding stuff, finish that.
        if let Some(j0) = cursors.oj.take() {
            let j0 = cursors.leading_equals + j0;
            let j = cursors.leading_equals + cursors.j;
            let i = cursors.leading_equals + cursors.i;
            debug!("adding from {} to {} / {}, context {}",
                   j0, j, b.len(), cursors.last_alive_context);

            if j0 < j {
                // Since we either always choose deletions
                // or always additions, we can't have two
                // consecutive replacements, hence there
                // should be no pending change.
                assert!(cursors.pending.is_none());
                cursors.pending = Pending::Addition(self.add_lines(
                    cursors.last_alive_context,
                    Some(i),
                    diff,
                    &b[j0..j],
                    cursors.trailing_equals
                ))
            }
        }
        if cursors.oi.is_none() {
            cursors.oi = Some(cursors.i)
        }
        cursors.i += 1
    }

    fn finish_i(&self, branch: &Branch, diff: &mut Diff<A>, b: &[&[u8]], cursors: &mut Cursors, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>) {
        let i = cursors.leading_equals + cursors.i;
        let j = cursors.leading_equals + cursors.j;
        if let Some(j0) = cursors.oj {
            // Before stopping, we were adding lines.
            let j0 = cursors.leading_equals + j0;
            if j0 < j {
                debug!("line {}, adding remaining lines before the last deletion i={} j={} j0={}",
                       line!(), i, j0, j);
                assert!(cursors.pending.is_none());
                cursors.pending = Pending::Addition(self.add_lines(
                    i - 1,
                    Some(i),
                    diff,
                    &b[j0..j],
                    cursors.trailing_equals
                ));
            }
        }

        let i0 = diff.lines_a.len() - cursors.trailing_equals;
        let dels = self.delete_lines(branch, diff, i, i0);
        stop_del(diff.file.clone(), cursors, dels, actions);
    }

    fn move_j(&self, branch: &Branch, diff: &mut Diff<A>, cursors: &mut Cursors) {
        // We will add things starting from j.
        // Are we currently deleting stuff?
        if let Some(i0) = cursors.oi.take() {
            let i0 = cursors.leading_equals + i0;
            let i = cursors.leading_equals + cursors.i;
            if i0 < i {
                assert!(cursors.pending.is_none());
                cursors.pending = Pending::Deletion(self.delete_lines(
                    branch,
                    diff,
                    i0,
                    i
                ))
            }
            cursors.last_alive_context = i0 - 1;
        }
        if cursors.oj.is_none() {
            cursors.oj = Some(cursors.j)
        }
        cursors.j += 1
    }

    fn finish_j(&self, branch: &Branch, diff: &mut Diff<A>, b: &[&[u8]], cursors: &mut Cursors, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>) {
        // There's a pending block to add at the end of the file.
        let j = cursors.leading_equals + cursors.j;
        let mut i = cursors.leading_equals + cursors.i;
        if let Some(i0) = cursors.oi {
            // We were doing a deletion when we stopped.
            let i0 = cursors.leading_equals + i0;
            if i0 < i {
                assert!(cursors.pending.is_none());
                cursors.pending = Pending::Deletion(self.delete_lines(
                    branch,
                    diff,
                    i0,
                    i
                ));
            }
            i = i0;
            debug!("line {}, adding lines after trailing equals: {:?} {:?}",
                   line!(), diff.lines_a.len(), cursors.trailing_equals);
        }

        let adds = self.add_lines(
            i - 1,
            if cursors.trailing_equals > 0 {
                Some(diff.lines_a.len() - cursors.trailing_equals)
            } else {
                None
            },
            diff,
            &b[j..b.len() - cursors.trailing_equals],
            cursors.trailing_equals
        );

        stop_add(diff.file.clone(), cursors, adds, actions)

    }

    fn local_diff<'a>(&'a self,
                      branch: &Branch,
                      actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>,
                      diff: &mut Diff<A>,
                      b: &[&'a [u8]]) {
        debug!("local_diff {} {}", diff.contents_a.len(), b.len());

        // Compute the costs.
        let mut cursors = bracket_equals(diff, b);

        debug!("equals: {:?}", cursors);

        let mut opt = Matrix::new(
            diff.contents_a.len() + 1 - cursors.leading_equals - cursors.trailing_equals,
            b.len() + 1 - cursors.leading_equals - cursors.trailing_equals,
            0
        );
        debug!("opt.rows: {:?}, opt.cols: {:?}", opt.rows, opt.cols);
        compute_costs(diff, b, cursors.leading_equals, cursors.trailing_equals, &mut opt);

        while cursors.i < opt.rows - 1 && cursors.j < opt.cols - 1 {
            debug!("i={}, j={}", cursors.i, cursors.j);
            let contents_a_i = diff.contents_a[cursors.leading_equals + cursors.i].clone();
            let contents_b_j: Value<'a, A> = Value::from_slice(b[cursors.leading_equals + cursors.j]);
            trace!("c_a_i = {:?} c_a_j = {:?}", contents_a_i, contents_b_j);
            if contents_a_i.eq(contents_b_j) {
                self.lines_eq(branch, diff, b, &mut cursors, actions)
            } else {
                // Else, the current lines on each side are not equal:
                debug!("not eq");
                if opt[cursors.i + 1][cursors.j] >= opt[cursors.i][cursors.j + 1] {
                    debug!("move i");
                    self.move_i(diff, b, &mut cursors)
                } else {
                    debug!("move j");
                    self.move_j(branch, diff, &mut cursors)
                }
            }
        }
        // Alright, we're at the end of either the original file, or the new version.
        debug!("i = {:?}, j = {:?}", cursors.i, cursors.j);
        // debug!("line_a {:?}, b {:?}", i, j, diff.lines_a, b);
        if cursors.i < opt.rows - 1 {
            // There are remaining deletions, i.e. things from the
            // original file are not in the new version.
            self.finish_i(branch, diff, b, &mut cursors, actions)
        } else if cursors.j < opt.cols - 1 {
             self.finish_j(branch, diff, b, &mut cursors, actions)
        }
    }

    pub fn diff<'a>(&'a self,
                    inode: Key<Option<Hash>>,
                    branch: &'a Branch,
                    file: Rc<PathBuf>,
                    line_num: &mut LineId,
                    actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>,
                    redundant: &mut Vec<(Key<PatchId>, Edge)>,
                    a: &mut Graph,
                    lines_b: &[&[u8]])
                    -> Result<()> {
        debug!("a = {:?}", a);
        let mut d = Diff {
            lines_a: Vec::new(),
            contents_a: Vec::new(),
            conflicts_ancestors: HashMap::new(),
            current_conflict_ancestor: Vec::new(),
            conflicts_descendants: HashMap::new(),
            conflicts_down_contexts: HashMap::new(),
            conflicts_sides: HashMap::new(),
            status: HashMap::new(),
            inode,
            file,
            line_num,
        };
        self.output_file(branch, &mut d, a, redundant)?;
        debug!("d = {:?}, {:?}", d.lines_a, d.contents_a);
        self.local_diff(
            branch,
            actions,
            &mut d,
            &lines_b
        );
        Ok(())
    }
}

// buf_b should initially contain the whole file.
pub fn read_lines(buf_b: &[u8]) -> Vec<&[u8]> {
    let mut lines_b = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while j < buf_b.len() {
        if buf_b[j] == 0xa {
            lines_b.push(&buf_b[i..j + 1]);
            i = j + 1
        }
        j += 1;
    }
    if i < j {
        lines_b.push(&buf_b[i..j])
    }
    lines_b
}


fn bracket_equals<A: Transaction>(diff: &Diff<A>, b: &[&[u8]]) -> Cursors {
    // Start by computing the leading and trailing equalities.
    let leading_equals = diff.contents_a.iter()
        .skip(1) // the first element is the "inode" node of the file.
        .zip(b.iter())
        .take_while(|&(a, b)| {
            trace!("leading_equals: {:?} = {:?} ?", a, b);
            let b: Value<A> = Value::from_slice(b);
            a.clone().eq(b)
        })
        .count();

    let trailing_equals =
        // Here, because we've skipped 1 element in `contents_a`,
        // while computing `leading_equals` above, we need to start
        // comparing from `leading_equals+1` in `contents_a`.
        if leading_equals+1 >= diff.contents_a.len() || leading_equals >= b.len() {
            0
        } else {
            (&diff.contents_a[leading_equals+1..]).iter().rev()
                .zip((&b[leading_equals..]).iter().rev())
                .take_while(|&(a, b)| {
                    trace!("trailing_equals: {:?} = {:?} ?", a, b);
                    let b: Value<A> = Value::from_slice(b);
                    a.clone().eq(b)
                })
                .count()
        };

    // Create the patches.
    Cursors {
        i: 1,
        j: 0,
        oi: None,
        oj: None,
        last_alive_context: leading_equals,
        pending: Pending::None,
        leading_equals,
        trailing_equals,
    }
}

fn compute_costs<A:Transaction>(diff: &Diff<A>, b: &[&[u8]], leading_equals: usize, trailing_equals: usize, opt: &mut Matrix<usize>) {
    if diff.contents_a.len() - trailing_equals - leading_equals > 0 {
        let mut i = diff.contents_a.len() - 1 - trailing_equals - leading_equals;
        loop {
            if b.len() - trailing_equals - leading_equals > 0 {
                let mut j = b.len() - 1 - trailing_equals - leading_equals;
                loop {
                    let contents_a_i = diff.contents_a[leading_equals + i].clone();
                    let contents_b_j: Value<A> = Value::from_slice(&b[leading_equals + j]);
                    opt[i][j] = if contents_a_i.eq(contents_b_j) {
                        opt[i + 1][j + 1] + 1
                    } else {
                        std::cmp::max(opt[i + 1][j], opt[i][j + 1])
                    };
                    if j > 0 {
                        j -= 1
                    } else {
                        break;
                    }
                }
            }
            if i > 0 {
                i -= 1
            } else {
                break;
            }
        }
    }
}

fn stop_del(file: Rc<PathBuf>, cursors: &mut Cursors, dels: Deletion, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>) {
    if let Pending::Addition(pending) = cursors.pending.take() {
        if let Some(del) = dels.del {
            actions.push(Record::Replace {
                dels: del,
                adds: pending,
                file: file.clone(),
                conflict_reordering: dels.conflict_ordering,
            })
        } else {
            actions.push(Record::Change {
                change: pending,
                file: file.clone(),
                conflict_reordering: dels.conflict_ordering,
            })
        }
    } else if let Some(del) = dels.del {
        actions.push(Record::Change {
            change: del,
            file: file.clone(),
            conflict_reordering: dels.conflict_ordering,
        })
    } else {
        for reord in dels.conflict_ordering {
            actions.push(Record::Change {
                change: reord,
                file: file.clone(),
                conflict_reordering: Vec::new(),
            })
        }
    }
}

fn stop_add(file: Rc<PathBuf>, cursors: &mut Cursors, adds: Change<Rc<RefCell<ChangeContext<PatchId>>>>, actions: &mut Vec<Record<Rc<RefCell<ChangeContext<PatchId>>>>>) {
    if let Pending::Deletion(pending) = cursors.pending.take() {
        if let Some(del) = pending.del {
            actions.push(Record::Replace {
                dels: del,
                adds: adds,
                file: file.clone(),
                conflict_reordering: pending.conflict_ordering,
            });
        } else {
            actions.push(Record::Change {
                change: adds,
                file: file.clone(),
                conflict_reordering: pending.conflict_ordering,
            })
        }
    } else {
        actions.push(Record::Change {
            change: adds,
            file: file.clone(),
            conflict_reordering: Vec::new(),
        })
    }

}
