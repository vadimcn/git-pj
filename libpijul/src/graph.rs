//! The data structure of the in-memory version of Pijul's main
//! datastructure, used to edit and organise it (for instance before a
//! record or before outputting a file).
use backend::*;
use Result;
use conflict;
use std::collections::{HashMap, HashSet};
use std::cmp::min;

use std;
use rand;

bitflags! {
    struct Flags: u8 {
        const LINE_HALF_DELETED = 4;
        const LINE_VISITED = 2;
        const LINE_ONSTACK = 1;
    }
}

/// The elementary datum in the representation of the repository state
/// at any given point in time. We need this structure (as opposed to
/// working directly on a branch) in order to add more data, such as
/// strongly connected component identifier, to each node.
#[derive(Debug)]
pub struct Line {
    /// The internal identifier of the line.
    pub key: Key<PatchId>,

    // The status of the line with respect to a dfs of
    // a graph it appears in. This is 0 or
    // `LINE_HALF_DELETED`.
    flags: Flags,
    children: usize,
    n_children: usize,
    index: usize,
    lowlink: usize,
    scc: usize,
}

impl Line {
    pub fn is_zombie(&self) -> bool {
        self.flags.contains(Flags::LINE_HALF_DELETED)
    }
}

/// A graph, representing the whole content of the repository state at
/// a point in time. The encoding is a "flat adjacency list", where
/// each vertex contains a index `children` and a number of children
/// `n_children`. The children of that vertex are then
/// `&g.children[children .. children + n_children]`.
#[derive(Debug)]
pub struct Graph {
    /// Array of all alive lines in the graph. Line 0 is a dummy line
    /// at the end, so that all nodes have a common successor
    pub lines: Vec<Line>,
    /// Edge + index of the line in the "lines" array above. "None"
    /// means "dummy line at the end", and corresponds to line number
    /// 0.
    children: Vec<(Option<Edge>, VertexId)>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct VertexId(usize);

const DUMMY_VERTEX: VertexId = VertexId(0);

impl std::ops::Index<VertexId> for Graph {
    type Output = Line;
    fn index(&self, idx: VertexId) -> &Self::Output {
        self.lines.index(idx.0)
    }
}
impl std::ops::IndexMut<VertexId> for Graph {
    fn index_mut(&mut self, idx: VertexId) -> &mut Self::Output {
        self.lines.index_mut(idx.0)
    }
}

use std::io::Write;

impl Graph {
    fn children(&self, i: VertexId) -> &[(Option<Edge>, VertexId)] {
        let ref line = self[i];
        &self.children[line.children..line.children + line.n_children]
    }

    fn child(&self, i: VertexId, j: usize) -> &(Option<Edge>, VertexId) {
        &self.children[self[i].children + j]
    }

    pub fn debug<W: Write>(
        &self,
        txn: &Txn,
        branch: &Branch,
        add_others: bool,
        introduced_by: bool,
        mut w: W,
    ) -> std::io::Result<()> {
        writeln!(w, "digraph {{")?;
        let mut cache = HashMap::new();
        if add_others {
            for (line, i) in self.lines.iter().zip(0..) {
                cache.insert(line.key, i);
            }
        }
        let mut others = HashSet::new();
        for (line, i) in self.lines.iter().zip(0..) {
            let contents = {
                if let Some(c) = txn.get_contents(line.key) {
                    let c = c.into_cow();
                    if let Ok(c) = std::str::from_utf8(&c) {
                        c.split_at(std::cmp::min(50, c.len())).0.to_string()
                    } else {
                        "<INVALID>".to_string()
                    }
                } else {
                    "".to_string()
                }
            };
            let contents = format!("{:?}", contents);
            let contents = contents.split_at(contents.len() - 1).0.split_at(1).1;
            writeln!(
                w,
                "n_{}[label=\"{}: {}.{}: {}\"];",
                i,
                i,
                line.key.patch.to_base58(),
                line.key.line.to_hex(),
                contents
            )?;

            if add_others && !line.key.is_root() {
                for (_, v) in txn.iter_nodes(branch, Some((line.key, None)))
                    .take_while(|&(k, _)| k == line.key)
                {
                    if let Some(dest) = cache.get(&v.dest) {
                        writeln!(
                            w,
                            "n_{} -> n_{}[color=red,label=\"{:?}{}{}\"];",
                            i,
                            dest,
                            v.flag.bits(),
                            if introduced_by { " " } else { "" },
                            if introduced_by {
                                v.introduced_by.to_base58()
                            } else {
                                String::new()
                            }
                        )?;
                    } else {
                        if !others.contains(&v.dest) {
                            others.insert(v.dest);
                            writeln!(
                                w,
                                "n_{}[label=\"{}.{}\",color=red];",
                                v.dest.to_base58(),
                                v.dest.patch.to_base58(),
                                v.dest.line.to_hex()
                            )?;
                        }
                        writeln!(
                            w,
                            "n_{} -> n_{}[color=red,label=\"{:?}{}{}\"];",
                            i,
                            v.dest.to_base58(),
                            v.flag.bits(),
                            if introduced_by { " " } else { "" },
                            if introduced_by {
                                v.introduced_by.to_base58()
                            } else {
                                String::new()
                            }
                        )?;
                    }
                }
            }
            for &(ref edge, VertexId(j)) in
                &self.children[line.children..line.children + line.n_children]
            {
                if let Some(ref edge) = *edge {
                    writeln!(
                        w,
                        "n_{}->n_{}[label=\"{:?}{}{}\"];",
                        i,
                        j,
                        edge.flag.bits(),
                        if introduced_by { " " } else { "" },
                        if introduced_by {
                            edge.introduced_by.to_base58()
                        } else {
                            String::new()
                        }
                    )?
                } else {
                    writeln!(w, "n_{}->n_{}[label=\"none\"];", i, j)?
                }
            }
        }
        writeln!(w, "}}")?;
        Ok(())
    }
}

use sanakirja::value::Value;
/// A "line outputter" trait.
pub trait LineBuffer<'a, T: 'a + Transaction> {
    fn output_line(&mut self, key: &Key<PatchId>, contents: Value<'a, T>) -> Result<()>;

    fn output_conflict_marker(&mut self, s: &'a str) -> Result<()>;
    fn begin_conflict(&mut self) -> Result<()> {
        self.output_conflict_marker(conflict::START_MARKER)
    }
    fn conflict_next(&mut self) -> Result<()> {
        self.output_conflict_marker(conflict::SEPARATOR)
    }
    fn end_conflict(&mut self) -> Result<()> {
        self.output_conflict_marker(conflict::END_MARKER)
    }
}

impl<'a, T: 'a + Transaction, W: std::io::Write> LineBuffer<'a, T> for W {
    fn output_line(&mut self, k: &Key<PatchId>, c: Value<T>) -> Result<()> {
        for chunk in c {
            debug!("output line {:?} {:?}", k, std::str::from_utf8(chunk));
            try!(self.write_all(chunk))
        }
        Ok(())
    }

    fn output_conflict_marker(&mut self, s: &'a str) -> Result<()> {
        try!(self.write(s.as_bytes()));
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Visits {
    pub first: usize,
    pub last: usize,
    pub begins_conflict: bool,
    pub ends_conflict: bool,
    pub has_brothers: bool,
}

impl Default for Visits {
    fn default() -> Self {
        Visits {
            first: 0,
            last: 0,
            ends_conflict: false,
            begins_conflict: false,
            has_brothers: false,
        }
    }
}

pub struct DFS {
    visits: Vec<Visits>,
    counter: usize,
    has_conflicts: bool,
}

impl DFS {
    pub fn new(n: usize) -> Self {
        DFS {
            visits: vec![Visits::default(); n],
            counter: 1,
            has_conflicts: false,
        }
    }
}

impl DFS {
    fn mark_discovered(&mut self, scc: usize) {
        if self.visits[scc].first == 0 {
            self.visits[scc].first = self.counter;
            self.counter += 1;
        }
    }

    fn mark_last_visit(&mut self, scc: usize) {
        self.mark_discovered(scc);
        self.visits[scc].last = self.counter;
        self.counter += 1;
    }

    fn first_visit(&self, scc: usize) -> usize {
        self.visits[scc].first
    }

    fn last_visit(&self, scc: usize) -> usize {
        self.visits[scc].last
    }

    fn ends_conflict(&mut self, scc: usize) {
        self.visits[scc].ends_conflict = true
    }
    fn begins_conflict(&mut self, scc: usize) {
        self.visits[scc].begins_conflict = true;
        self.has_conflicts = true
    }
    fn has_brothers(&mut self, scc: usize) {
        self.visits[scc].has_brothers = true
    }
}

impl Graph {
    /// Tarjan's strongly connected component algorithm, returning a
    /// vector of strongly connected components, where each SCC is a
    /// vector of vertex indices.
    fn tarjan(&mut self) -> Vec<Vec<VertexId>> {
        if self.lines.len() <= 1 {
            return vec![vec![VertexId(0)]];
        }

        let mut call_stack = vec![(VertexId(1), 0, true)];

        let mut index = 0;
        let mut stack = Vec::new();
        let mut scc = Vec::new();
        while let Some((n_l, i, first_visit)) = call_stack.pop() {
            if first_visit {
                // First time we visit this node.
                let ref mut l = self[n_l];
                debug!("tarjan: {:?}", l.key);
                (*l).index = index;
                (*l).lowlink = index;
                (*l).flags = (*l).flags | Flags::LINE_ONSTACK | Flags::LINE_VISITED;
                debug!("{:?} {:?} chi", (*l).key, (*l).n_children);
                stack.push(n_l);
                index = index + 1;
            } else {
                let &(_, n_child) = self.child(n_l, i);
                self[n_l].lowlink = std::cmp::min(self[n_l].lowlink, self[n_child].lowlink);
            }

            let call_stack_length = call_stack.len();
            for j in i..self[n_l].n_children {
                let &(_, n_child) = self.child(n_l, j);
                if !self[n_child].flags.contains(Flags::LINE_VISITED) {
                    call_stack.push((n_l, j, false));
                    call_stack.push((n_child, 0, true));
                    break;
                // self.tarjan_dfs(scc, stack, index, n_child);
                } else {
                    if self[n_child].flags.contains(Flags::LINE_ONSTACK) {
                        self[n_l].lowlink = min(self[n_l].lowlink, self[n_child].index)
                    }
                }
            }
            if call_stack_length < call_stack.len() {
                // recursive call
                continue;
            }
            // Here, all children of n_l have been visited.

            if self[n_l].index == self[n_l].lowlink {
                let mut v = Vec::new();
                loop {
                    match stack.pop() {
                        None => break,
                        Some(n_p) => {
                            self[n_p].scc = scc.len();
                            self[n_p].flags = self[n_p].flags ^ Flags::LINE_ONSTACK;
                            v.push(n_p);
                            if n_p == n_l {
                                break;
                            }
                        }
                    }
                }
                scc.push(v);
            }
        }
        scc
    }

    /// Run a depth-first search on this graph, assigning the
    /// `first_visit` and `last_visit` numbers to each node.
    fn dfs<A: Transaction, R>(
        &mut self,
        txn: &GenericTxn<A, R>,
        branch: &Branch,
        scc: &[Vec<VertexId>],
        dfs: &mut DFS,
        forward: &mut Vec<(Key<PatchId>, Edge)>,
    ) {
        let mut call_stack: Vec<(_, HashSet<usize>, _, _)> =
            vec![(scc.len() - 1, HashSet::new(), true, None)];
        while let Some((n_scc, mut forward_scc, is_first_child, descendants)) = call_stack.pop() {
            debug!("dfs, n_scc = {:?}", n_scc);
            for &VertexId(id) in scc[n_scc].iter() {
                debug!("dfs, n_scc: {:?}", self.lines[id].key);
            }
            dfs.mark_discovered(n_scc);
            debug!(
                "scc: {:?}, first {} last {}",
                n_scc,
                dfs.first_visit(n_scc),
                dfs.last_visit(n_scc)
            );
            let mut descendants = if let Some(descendants) = descendants {
                descendants
            } else {
                // First visit / discovery of SCC n_scc.

                // After Tarjan's algorithm, the SCC numbers are in reverse
                // topological order.
                //
                // Here, we want to visit the first child in topological
                // order, hence the one with the largest SCC number first.
                //

                // Collect all descendants of this SCC, in order of increasing
                // SCC.
                let mut descendants = Vec::new();
                for cousin in scc[n_scc].iter() {
                    for &(_, n_child) in self.children(*cousin) {
                        let child_component = self[n_child].scc;
                        if child_component < n_scc {
                            // If this is a child and not a sibling.
                            descendants.push(child_component)
                        }
                    }
                }
                descendants.sort();
                debug!("sorted descendants: {:?}", descendants);
                descendants
            };

            // SCCs to which we have forward edges.
            let mut recursive_call = None;
            while let Some(child) = descendants.pop() {
                debug!(
                    "child {:?}, first {} last {}",
                    child,
                    dfs.first_visit(child),
                    dfs.last_visit(child)
                );
                if dfs.first_visit(child) == 0 {
                    // This SCC has not yet been visited, visit it.
                    recursive_call = Some(child);
                    if !is_first_child {
                        // Two incomparable children, n_scc starts a conflict
                        debug!("!is_first_child, n_scc = {:?}", n_scc);
                        dfs.begins_conflict(n_scc);
                        dfs.has_brothers(child)
                    }
                    break;
                } else if dfs.last_visit(child) < dfs.first_visit(n_scc) {
                    dfs.ends_conflict(child)
                } else if dfs.first_visit(n_scc) < dfs.first_visit(child) {
                    // This is a forward edge.
                    debug!("last_visit to {:?}: {:?}", child, dfs.last_visit(child));
                    forward_scc.insert(child);
                }
            }
            if let Some(child) = recursive_call {
                call_stack.push((n_scc, forward_scc, false, Some(descendants)));
                call_stack.push((child, HashSet::new(), true, None));
            } else {
                dfs.mark_last_visit(n_scc);
                // After this, collect forward edges.
                for cousin in scc[n_scc].iter() {
                    for &(ref edge, n_child) in self.children(*cousin) {
                        if let Some(mut edge) = *edge {
                            // Is this edge a forward edge of the DAG?
                            if forward_scc.contains(&self[n_child].scc)
                                && edge.flag.contains(EdgeFlags::PSEUDO_EDGE)
                                && !txn.test_edge(
                                    branch,
                                    self[*cousin].key,
                                    edge.dest,
                                    EdgeFlags::DELETED_EDGE,
                                    EdgeFlags::DELETED_EDGE,
                                ) {
                                    debug!("forward: {:?} {:?}", self[*cousin].key, edge);
                                    forward.push((self[*cousin].key, edge))
                            }

                            // Does this edge have parallel PSEUDO edges?
                            edge.flag = EdgeFlags::PSEUDO_EDGE;
                            edge.introduced_by = ROOT_PATCH_ID;
                            let mut edges = txn.iter_nodes(
                                branch,
                                Some((self[*cousin].key, Some(&edge))),
                            ).take_while(|&(k, v)| {
                                k == self[*cousin].key && v.dest == edge.dest
                                    && v.flag <= EdgeFlags::FOLDER_EDGE | EdgeFlags::PSEUDO_EDGE
                            });
                            edges.next(); // ignore the first pseudo-edge.
                            forward.extend(edges.map(|(k, &v)| (k, v))); // add all parallel edges.
                        }
                    }
                }
            }
        }
    }
}

impl<A: Transaction, R> GenericTxn<A, R> {
    /// This function constructs a graph by reading the branch from the
    /// input key. It guarantees that all nodes but the first one (index
    /// 0) have a common descendant, which is index 0.
    pub fn retrieve<'a>(&'a self, branch: &Branch, key0: Key<PatchId>) -> Graph {
        let mut graph = Graph {
            lines: Vec::new(),
            children: Vec::new(),
        };
        // Insert last "dummy" line (so that all lines have a common descendant).
        graph.lines.push(Line {
            key: ROOT_KEY,
            flags: Flags::empty(),
            children: 0,
            n_children: 0,
            index: 0,
            lowlink: 0,
            scc: 0,
        });

        // Avoid the root key.
        let mut cache: HashMap<Key<PatchId>, VertexId> = HashMap::new();
        cache.insert(ROOT_KEY.clone(), DUMMY_VERTEX);
        let mut stack = Vec::new();
        if self.get_nodes(&branch, key0, None).is_some() {
            stack.push(key0)
        }
        while let Some(key) = stack.pop() {
            if cache.contains_key(&key) {
                // We're doing a DFS here, this can definitely happen.
                continue;
            }

            let idx = VertexId(graph.lines.len());
            cache.insert(key.clone(), idx);

            debug!("{:?}", key);
            let mut is_zombie = false;
            // Does this vertex have a DELETED/DELETED+FOLDER edge
            // pointing to it?
            let mut first_edge = Edge::zero(EdgeFlags::PARENT_EDGE | EdgeFlags::DELETED_EDGE);
            let mut nodes = self.iter_nodes(&branch, Some((key, Some(&first_edge))));
            if let Some((k, v)) = nodes.next() {
                debug!("zombie? {:?} {:?}", k, v);
                if k == key
                    && (v.flag | EdgeFlags::FOLDER_EDGE == first_edge.flag | EdgeFlags::FOLDER_EDGE)
                {
                    // Does this vertex also have an alive edge
                    // pointing to it? (might not be the case for the
                    // first vertex)
                    if key == key0 {
                        first_edge.flag = EdgeFlags::PARENT_EDGE;
                        nodes = self.iter_nodes(&branch, Some((key, Some(&first_edge))));
                        if let Some((_, v)) = nodes.next() {
                            // We know the key is `key`.
                            is_zombie = v.flag | EdgeFlags::FOLDER_EDGE
                                == first_edge.flag | EdgeFlags::FOLDER_EDGE
                        }
                    } else {
                        is_zombie = true
                    }
                }
            }
            debug!("is_zombie: {:?}", is_zombie);
            let mut l = Line {
                key: key.clone(),
                flags: if is_zombie {
                    Flags::LINE_HALF_DELETED
                } else {
                    Flags::empty()
                },
                children: graph.children.len(),
                n_children: 0,
                index: 0,
                lowlink: 0,
                scc: 0,
            };

            let mut last_flag = EdgeFlags::empty();
            let mut last_dest = ROOT_KEY;

            for (_, v) in self.iter_nodes(&branch, Some((key, None)))
                .take_while(|&(k, v)| {
                    k == key
                        && v.flag
                            <= EdgeFlags::PSEUDO_EDGE | EdgeFlags::FOLDER_EDGE
                                | EdgeFlags::EPSILON_EDGE
                }) {
                debug!("-> v = {:?}", v);
                if last_flag == EdgeFlags::PSEUDO_EDGE && last_dest == v.dest {
                    // This is a doubled edge, it should be removed.
                } else {
                    graph.children.push((Some(v.clone()), DUMMY_VERTEX));
                    l.n_children += 1;
                    if !cache.contains_key(&v.dest) {
                        stack.push(v.dest.clone())
                    } else {
                        debug!("v already visited");
                    }
                    last_flag = v.flag;
                    last_dest = v.dest;
                }
            }
            // If this key has no children, give it the dummy child.
            if l.n_children == 0 {
                debug!("no children for {:?}", l);
                graph.children.push((None, DUMMY_VERTEX));
                l.n_children = 1;
            }
            graph.lines.push(l)
        }
        for &mut (ref child_key, ref mut child_idx) in graph.children.iter_mut() {
            if let Some(ref child_key) = *child_key {
                if let Some(idx) = cache.get(&child_key.dest) {
                    *child_idx = *idx
                }
            }
        }
        graph
    }
}

/// The conflict markers keep track of the number of conflicts, and is
/// used for outputting conflicts to a given LineBuffer.
///
/// "Zombie" conflicts are those conflicts introduced by zombie
/// vertices in the contained Graph.
struct ConflictMarkers<'b> {
    current_is_zombie: bool,
    current_conflicts: usize,
    graph: &'b Graph,
}

impl<'b> ConflictMarkers<'b> {
    fn output_zombie_markers_if_needed<'a, A: Transaction + 'a, B: LineBuffer<'a, A>>(
        &mut self,
        buf: &mut B,
        vertex: VertexId,
    ) -> Result<()> {
        if self.graph[vertex].is_zombie() {
            if !self.current_is_zombie {
                debug!("begin zombie conflict: vertex = {:?}", vertex);
                self.current_is_zombie = true;
                buf.begin_conflict()?;
            }
        } else if self.current_is_zombie {
            // Zombie segment has ended
            self.current_is_zombie = false;
            buf.end_conflict()?;
        }
        Ok(())
    }

    fn begin_conflict<'a, A: Transaction + 'a, B: LineBuffer<'a, A>>(
        &mut self,
        buf: &mut B,
    ) -> Result<()> {
        buf.begin_conflict()?;
        self.current_conflicts += 1;
        Ok(())
    }
    fn end_conflict<'a, A: Transaction + 'a, B: LineBuffer<'a, A>>(
        &mut self,
        buf: &mut B,
    ) -> Result<()> {
        if self.current_conflicts > 0 {
            buf.end_conflict()?;
            self.current_conflicts -= 1;
        }
        Ok(())
    }
}

/// In the case of nested conflicts, a single conflict sometimes needs
/// to be treated like a line.
#[derive(Debug, Clone)]
enum ConflictLine {
    Conflict(Vec<Vec<ConflictLine>>),
    Line(VertexId),
}

impl<'a, A: Transaction + 'a, R> GenericTxn<A, R> {
    fn output_conflict<B: LineBuffer<'a, A>>(
        &'a self,
        conflicts: &mut ConflictMarkers,
        buf: &mut B,
        graph: &Graph,
        scc: &[Vec<VertexId>],
        conflict: &mut [Vec<ConflictLine>],
    ) -> Result<()> {
        let mut is_first = true;
        let n_sides = conflict.len();
        if n_sides > 1 {
            conflicts.begin_conflict(buf)?;
        }
        for side in conflict {
            if !is_first {
                buf.conflict_next()?;
            }
            is_first = false;
            debug!("side = {:?}", side);
            for i in side {
                match *i {
                    ConflictLine::Line(i) => {
                        conflicts.output_zombie_markers_if_needed(buf, i)?;
                        let key = graph[i].key;
                        if let Some(cont) = self.get_contents(key) {
                            debug!("outputting {:?}", cont);
                            buf.output_line(&key, cont)?;
                        }
                    }
                    ConflictLine::Conflict(ref mut c) => {
                        debug!("begin conflict {:?}", c);
                        sort_conflict(graph, c);
                        self.output_conflict(conflicts, buf, graph, scc, c)?;
                        debug!("end conflict");
                    }
                }
            }
        }
        if n_sides > 1 {
            conflicts.end_conflict(buf)?;
        }
        Ok(())
    }

    pub fn output_file<B: LineBuffer<'a, A>>(
        &'a self,
        branch: &Branch,
        buf: &mut B,
        graph: &mut Graph,
        forward: &mut Vec<(Key<PatchId>, Edge)>,
    ) -> Result<bool> {
        debug!("output_file");
        if graph.lines.len() <= 1 {
            return Ok(false)
        }
        let scc = graph.tarjan(); // SCCs are given here in reverse order.
        debug!("There are {} SCC", scc.len());
        debug!("SCCs = {:?}", scc);

        let mut dfs = DFS::new(scc.len());
        graph.dfs(self, branch, &scc, &mut dfs, forward);

        debug!("dfs done");

        buf.output_line(&graph.lines[1].key, Value::from_slice(b""))?;
        let mut conflict_tree = conflict_tree(graph, &scc, &dfs)?;
        debug!("conflict_tree = {:?}", conflict_tree);
        let mut conflicts = ConflictMarkers {
            current_is_zombie: false,
            current_conflicts: 0,
            graph: &graph,
        };
        self.output_conflict(&mut conflicts, buf, graph, &scc, &mut conflict_tree)?;
        // Close any remaining zombie part (if needed).
        conflicts.output_zombie_markers_if_needed(buf, DUMMY_VERTEX)?;
        debug!("/output_file");
        Ok(dfs.has_conflicts)
    }
}


fn side_ends_at(graph: &Graph, side: &[ConflictLine], id: VertexId) -> bool {
    for j in side.iter() {
        match *j {
            ConflictLine::Line(j) => {
                if graph.children(j).iter().any(|&(_, x)| x == id) {
                    return true
                }
            }
            ConflictLine::Conflict(ref sides) => {
                if sides.iter().any(|side| side_ends_at(graph, side, id)) {
                    return true
                }
            }
        }
    }
    false
}

fn conflict_tree(
    graph: &Graph,
    scc: &[Vec<VertexId>],
    dfs: &DFS,
) -> Result<Vec<Vec<ConflictLine>>> {
    let mut i = scc.len() - 1;

    let mut stack: Vec<Vec<Vec<ConflictLine>>> = vec![Vec::new()];
    loop {
        if dfs.visits[i].ends_conflict && stack.len() > 1 {
            debug!("End conflict {:?}", dfs.visits[i]);
            let conflict = stack.pop().unwrap();

            // First determine if all sides are ended by the same
            // line.

            let mut remaining_sides = Vec::new();
            let mut current_conflict = Vec::with_capacity(conflict.len());
            for side in conflict.iter() {
                // Is this side part of the conflict stopped by
                // this line?
                if side_ends_at(graph, side, scc[i][0]) {
                    current_conflict.push(side.clone())
                } else {
                    remaining_sides.push(side.clone())
                }
            }
            debug!("remaining sides = {:?}", remaining_sides);
            if !remaining_sides.is_empty() {
                // If there are remaining sides to the conflict, then
                // the sides that ended here must necessarily be
                // placed at the beginning of a side of the parent
                // conflict, for else this algorithm would not have
                // characterised all sides (i.e. all sides from both
                // `remaining_sides` and `current_conflict`) as part
                // of the same conflict.
                remaining_sides.push(vec![ConflictLine::Conflict(current_conflict)]);
                stack.push(remaining_sides);
            } else {
                // If this line ends the current conflict completely,
                // let's append the conflict at the end of the last
                // side of the current "pseudo-conflict" (i.e. a
                // conflict with one or more sides)
                if let Some(ref mut last) = stack.last_mut() {
                    if let Some(ref mut last) = last.last_mut() {
                        last.push(ConflictLine::Conflict(current_conflict))
                    }
                }
            }
        } else if dfs.visits[i].has_brothers {
            if let Some(ref mut top) = stack.last_mut() {
                // top is the last conflict.
                top.push(Vec::new())
            }
        }

        if scc[i].len() > 1 {
            debug!("cycle conflict: {:?}", scc);
            // In a cyclic conflict, sides are exactly the members of
            // `scc[i]`.
            if let Some(ref mut last) = stack.last_mut() {
                if let Some(ref mut last) = last.last_mut() {
                    last.push(ConflictLine::Conflict(
                        scc[i]
                            .iter()
                            .map(|&x| vec![ConflictLine::Line(x)])
                            .collect(),
                    ))
                }
            }
        } else if scc[i].len() == 1 {
            let key = graph[scc[i][0]].key;
            debug!("scc[{}].len() = 1, key = {:?}", i, key);
            if key != ROOT_KEY {
                if let Some(ref mut top) = stack.last_mut() {
                    // top is the last conflict.
                    if top.is_empty() {
                        top.push(Vec::new())
                    }
                    if let Some(ref mut last_side) = top.last_mut() {
                        last_side.push(ConflictLine::Line(scc[i][0]))
                    }
                }
            }
        }

        if dfs.visits[i].begins_conflict {
            debug!("Begin conflict {:?} {:?}", i, dfs.visits[i]);
            stack.push(Vec::new());
        }

        if i == 0 {
            break;
        } else {
            i -= 1
        }
    }
    Ok(stack.pop().unwrap())
}

fn sort_conflict(graph: &Graph, conflict: &mut [Vec<ConflictLine>]) {
    conflict.sort_by(|a, b| first_line(graph, a).cmp(&first_line(graph, b)))
}

fn first_line(graph: &Graph, conflict: &[ConflictLine]) -> Option<Key<PatchId>> {
    let mut result = None;
    for side in conflict {
        match *side {
            ConflictLine::Line(l) => {
                result = if result.is_none() {
                    Some(graph[l].key)
                } else {
                    min(result, Some(graph[l].key))
                }
            }
            ConflictLine::Conflict(ref c) => {
                let first = c.iter().filter_map(|side| first_line(graph, side)).min();
                if result.is_none() {
                    result = first
                } else if first.is_some() {
                    result = min(result, first)
                }
            }
        }
    }
    result
}

impl<'env, R: rand::Rng> MutTxn<'env, R> {

    pub fn remove_redundant_edges(
        &mut self,
        branch: &mut Branch,
        forward: &[(Key<PatchId>, Edge)],
    ) -> Result<()> {
        for &(key, edge) in forward.iter() {
            self.del_nodes_with_rev(branch, key, edge)?;
        }
        Ok(())
    }
}
