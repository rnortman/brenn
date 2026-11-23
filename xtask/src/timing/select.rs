//! Which runs a view reads: by checkout, and by run id or a unique prefix of
//! one.

use std::path::Path;

use super::TimingError;
use super::store::{self, RunHeader};

/// The checkout a view is limited to, or none for every checkout sharing the
/// store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub checkout: Option<String>,
}

impl Scope {
    pub fn admits(&self, header: &RunHeader) -> bool {
        match &self.checkout {
            Some(checkout) => header.checkout == *checkout,
            None => true,
        }
    }

    /// The scope as words for a heading or an empty result.
    pub fn describe(&self) -> String {
        match &self.checkout {
            Some(checkout) => format!("from {checkout}"),
            None => "from any checkout".to_string(),
        }
    }
}

/// The one run whose id starts with `prefix`, in any checkout: an id names a
/// run on the whole machine.
pub fn resolve(store_root: &Path, prefix: &str) -> Result<String, TimingError> {
    let ids = store::run_ids(store_root)?;
    let matching: Vec<&String> = ids.iter().filter(|id| id.starts_with(prefix)).collect();
    match matching.as_slice() {
        [id] => Ok((*id).clone()),
        [] => Err(TimingError::Select(format!(
            "no run id starts with {prefix:?}"
        ))),
        many => Err(TimingError::Select(format!(
            "{} run ids start with {prefix:?}; give more of it ({} ... {})",
            many.len(),
            many[0],
            many[many.len() - 1]
        ))),
    }
}

/// The newest run the scope admits. Only run headers are read; a run without
/// one names no checkout and is passed over.
pub fn newest(store_root: &Path, scope: &Scope) -> Result<Option<String>, TimingError> {
    for id in store::run_ids(store_root)?.into_iter().rev() {
        if let Some(header) = store::load_header(store_root, &id)?
            && scope.admits(&header)
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::timing::store::runs_dir;
    use crate::timing::store::tests::{ID_A, ID_B, header_text, write_run};

    pub(crate) const ID_C: &str = "20261122T120000.000000001Z-300";
    pub(crate) const OTHER: &str = "/src/alice/other";

    /// A run directory with a header from `checkout`.
    pub(crate) fn write_run_in(store: &Path, id: &str, checkout: &str) -> std::path::PathBuf {
        let dir = runs_dir(store).join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("run"), header_text(id, checkout)).unwrap();
        dir
    }

    fn here() -> Scope {
        Scope {
            checkout: Some("/src/alice/brenn".into()),
        }
    }

    #[test]
    fn newest_is_the_scopes_and_passes_over_other_checkouts_and_headerless_runs() {
        let store = tempfile::tempdir().unwrap();
        assert_eq!(newest(store.path(), &here()).unwrap(), None);
        write_run(store.path(), ID_A);
        write_run_in(store.path(), ID_B, OTHER);
        std::fs::create_dir_all(runs_dir(store.path()).join(ID_C)).unwrap();
        assert_eq!(
            newest(store.path(), &here()).unwrap().as_deref(),
            Some(ID_A)
        );
        let all = Scope { checkout: None };
        assert_eq!(newest(store.path(), &all).unwrap().as_deref(), Some(ID_B));
    }

    #[test]
    fn a_prefix_resolves_to_one_run_or_is_an_error() {
        let store = tempfile::tempdir().unwrap();
        write_run(store.path(), ID_A);
        write_run_in(store.path(), ID_B, OTHER);
        assert_eq!(resolve(store.path(), "20261122T11").unwrap(), ID_B);
        assert_eq!(resolve(store.path(), ID_A).unwrap(), ID_A);
        let TimingError::Select(m) = resolve(store.path(), "20261122T1").unwrap_err() else {
            panic!("a selection error");
        };
        assert!(m.starts_with("2 run ids start with"), "{m}");
        assert!(matches!(
            resolve(store.path(), "2025").unwrap_err(),
            TimingError::Select(_)
        ));
    }
}
