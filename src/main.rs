extern crate libpijul;
extern crate git2;
extern crate rand;
extern crate chrono;
extern crate env_logger;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
use chrono::TimeZone;
use git2::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use clap::{App, Arg};
use std::path::Path;
use libpijul::patch::PatchFlags;

fn main() {
    env_logger::init();
    let matches = App::new("Git -> Pijul converter")
        .version(crate_version!())
        .about("Converts a Git repository into a Pijul one")
        .arg(Arg::with_name("INPUT")
             .help("Sets the input Git repository.")
             .required(true)
             .index(1))
        .get_matches();

    let path = Path::new(matches.value_of("INPUT").unwrap());

    let repo = Repository::open(path).unwrap();
    let commits = get_commits(&repo);

    let pijul_dir = path.join(".pijul");
    let pristine_dir = pijul_dir.join("pristine");

    std::fs::remove_dir_all(&pijul_dir).unwrap_or(());
    std::fs::create_dir_all(&pristine_dir).unwrap();

    for &(commit_id, ref branch, ref forks) in commits.iter() {

        debug!("id {:?}", commit_id);
        let commit = repo.find_commit(commit_id).unwrap();
        let mut checkout = build::CheckoutBuilder::new();
        checkout.force();
        repo.checkout_tree(commit.as_object(), Some(&mut checkout)).unwrap();

        let mut increase = 409600;
        let _res = loop {
            match file_moves(&repo, &commit, &pristine_dir, increase) {
                Err(ref e) if e.lacks_space() => { increase *= 2 },
                e => break e
            }
        };

        let author = commit.author();
        record(path, &branch,
               libpijul::PatchHeader {
                   authors: vec![author.name().unwrap().to_string()],
                   name: commit.message().unwrap().to_string(),
                   description: None,
                   timestamp: chrono::Utc.timestamp(author.when().seconds(), 0),
                   flag: PatchFlags::empty(),
               });

        let new_repo = libpijul::Repository::open(&pristine_dir, None).unwrap();

        for fork in forks {
            let mut txn = new_repo.mut_txn_begin(rand::thread_rng()).unwrap();

            let branch = txn.open_branch(&branch).unwrap();
            let new_branch = txn.fork(&branch, &fork).unwrap();
            let _res1 = txn.commit_branch(branch);
            let _res2 = txn.commit_branch(new_branch);
            let _res3 = txn.commit();
        }
    }
}

fn get_commits(repo: &git2::Repository) -> Vec<(git2::Oid, std::string::String, std::vec::Vec<std::string::String>)> {
    let mut walk = repo.revwalk().unwrap();
    walk.set_sorting(git2::SORT_TOPOLOGICAL);

    let mut commit_to_branch = HashMap::new();
    for branch in repo.branches(None).unwrap() {
        let branch = branch.unwrap().0;
        let commit = branch.get().target().unwrap();
        let _res = walk.push(commit);
        commit_to_branch.insert(commit, (branch.name().unwrap().unwrap().to_owned(), Vec::new()));
    }

    let mut commits_reverse = Vec::new();
    for commit in walk {
        let commit = commit.unwrap();
        let (current_branch, forks) = commit_to_branch.get(&commit).unwrap().clone();

        for parent in repo.find_commit(commit).unwrap().parents() {
            match commit_to_branch.entry(parent.id()) {
                // put the parent into the same branch as the child, if it's not already on one
                Vacant(entry) => {
                    entry.insert((current_branch.clone(), Vec::new()));
                },
                // otherwise this is a fork point at the parent
                Occupied(mut entry) => {
                    let ref mut parent_forks = entry.get_mut().1;
                    parent_forks.push(current_branch.clone());
                },
            };
        }

        debug!("commit {:?} in branch {:?} with forks {:?}", commit, current_branch, forks);
        commits_reverse.push((commit, current_branch, forks));
    }

    commits_reverse.reverse();
    commits_reverse
}

fn file_moves(repo: &Repository, commit: &Commit, pristine_dir: &Path, increase: u64)
                    -> libpijul::Result<()> {
    debug!("file_moves, commit {:?}", commit.id());
    debug!("commit msg: {:?}", commit.message());

    let tree1 = commit.tree().unwrap();
    let new_repo = match libpijul::Repository::open(&pristine_dir, Some(increase)) {
        Ok(repo) => repo,
        Err(x) => return Err(x)
    };
    let mut txn = new_repo.mut_txn_begin(rand::thread_rng()).unwrap();

    let mut has_parents = false;
    for parent in commit.parents() {
        has_parents = true;
        debug!("parent: {:?}", parent.id());
        let tree0 = parent.tree().unwrap();
        let mut diff = repo.diff_tree_to_tree(Some(&tree0), Some(&tree1), None).unwrap();
        diff.find_similar(None).unwrap();

        diff.foreach(&mut |delta, _| file_cb(&mut txn, delta), None, None, None).unwrap();
    }

    if !has_parents {
        let mut diff = repo.diff_tree_to_tree(None, Some(&tree1), None).unwrap();
        diff.find_similar(None).unwrap();
        diff.foreach(&mut |delta, _| file_cb(&mut txn, delta), None, None, None).unwrap();
    }

    txn.commit().unwrap();

    Ok(())
}

fn file_cb<R:rand::Rng>(txn: &mut libpijul::MutTxn<R>, delta: DiffDelta) -> bool {
    debug!("nfiles: {:?}", delta.nfiles());
    debug!("old: {:?}", delta.old_file().path());
    debug!("new: {:?}", delta.new_file().path());
    debug!("status {:?}", delta.status());
    match delta.status() {
        Delta::Renamed => {
            let old = delta.old_file().path().unwrap();
            let new = delta.new_file().path().unwrap();
            debug!("moving {:?} to {:?}", old, new);
            txn.move_file(old, new, false).unwrap();
        }
        Delta::Added => {
            let path = delta.new_file().path().unwrap();
            debug!("added {:?}", path);
            let m = std::fs::metadata(&path).unwrap();
            txn.add_file(&path, m.is_dir()).unwrap_or(())
        }
        Delta::Deleted => {
            let path = delta.new_file().path().unwrap();
            debug!("deleted {:?}", path);
            txn.remove_file(path).unwrap()
        }
        _ => {}
    }
    true
}


fn record(output: &Path, branch_name: &str, header: libpijul::PatchHeader) {

    let (patch, hash, sync) = {

        let new_repo = libpijul::Repository::open(output.join(".pijul").join("pristine"), None).unwrap();

        let mut new_txn = new_repo.mut_txn_begin(rand::thread_rng()).unwrap();
        use libpijul::*;
        let mut record = RecordState::new();
        new_txn.record(&mut record, branch_name, output, None).unwrap();
        let (changes, sync) = record.finish();
        let changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
        let branch = new_txn.get_branch(branch_name).unwrap();
        let patch = new_txn.new_patch(
            &branch,
            header.authors.clone(),
            header.name.clone(),
            header.description.clone(),
            header.timestamp.clone(),
            changes,
            std::iter::empty(), // extra_deps.into_iter(),
            PatchFlags::empty()
        );

        let patches_dir = output.join(".pijul").join("patches");
        std::fs::create_dir_all(&patches_dir).unwrap();
        let hash = patch.save(&patches_dir, None).unwrap();
        new_txn.commit().unwrap();
        (patch, hash, sync)
    };
    debug!("hash recorded: {:?}",hash);
    let mut increase = 409600;
    let pristine_dir = output.join(".pijul").join("pristine");
    let res = loop {
        match record_no_resize(&pristine_dir, &output, branch_name, &hash, &patch, &sync, increase) {
            Err(ref e) if e.lacks_space() => { increase *= 2 },
            e => break e
        }
    };
    res.unwrap();
}

fn record_no_resize(pristine_dir: &Path, r: &Path, branch_name: &str, hash: &libpijul::Hash,
                        patch: &libpijul::Patch, syncs: &[libpijul::InodeUpdate], increase: u64)
                        -> libpijul::Result<Option<libpijul::Hash>> {

    use libpijul::*;
    let size_increase = increase + patch.size_upper_bound() as u64;
    let repo = match Repository::open(&pristine_dir, Some(size_increase)) {
        Ok(repo) => repo,
        Err(x) => return Err(x)
    };
    let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
    // save patch
    txn.apply_local_patch(&branch_name, r, &hash, &patch, &syncs, false)?;
    txn.commit()?;
    debug!("Recorded patch {}", hash.to_base58());
    Ok(Some(hash.clone()))
}
