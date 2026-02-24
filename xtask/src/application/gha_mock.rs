use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::gha_ops::Ops;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Call {
    RunCmd {
        program: String,
        args: Vec<String>,
        cwd: Option<PathBuf>,
    },
    CmdSuccess {
        program: String,
        args: Vec<String>,
    },
    ReadFile {
        path: PathBuf,
    },
    WriteFile {
        path: PathBuf,
        content: String,
    },
    GitCheckout {
        branch: String,
    },
    GitHasChanges {
        paths: Vec<String>,
    },
    CommitAndPush {
        branch: String,
        message: String,
        paths: Vec<String>,
    },
    GhaOutput {
        key: String,
        value: String,
    },
    GhaOutputMultiline {
        key: String,
        value: String,
    },
}

impl Call {
    pub(crate) fn label(&self) -> String {
        match self {
            Call::RunCmd { program, args, .. } => cmd_string(program, args),
            Call::CmdSuccess { program, args } => format!("[check] {}", cmd_string(program, args)),
            Call::ReadFile { path } => format!("[read_file] {}", path.display()),
            Call::WriteFile { path, .. } => format!("[write_file] {}", path.display()),
            Call::GitCheckout { branch } => format!("[git] checkout {branch}"),
            Call::GitHasChanges { .. } => "[git] has_changes".to_string(),
            Call::CommitAndPush { branch, .. } => format!("[git] commit_and_push {branch}"),
            Call::GhaOutput { key, .. } => format!("[gha] {key}"),
            Call::GhaOutputMultiline { key, .. } => format!("[gha:ml] {key}"),
        }
    }
}

fn cmd_string(program: &str, args: &[String]) -> String {
    if args.is_empty() {
        program.to_string()
    } else {
        format!("{program} {}", args.join(" "))
    }
}

// ── MockOps ──────────────────────────────────────────────────────────────

pub(crate) struct MockOps {
    calls: RefCell<Vec<Call>>,
    labeled_cmd_outputs: RefCell<Vec<(String, String)>>,
    labeled_cmd_statuses: RefCell<Vec<(String, bool)>>,
    files: RefCell<HashMap<PathBuf, String>>,
    git_has_changes_results: RefCell<Vec<bool>>,
}

impl MockOps {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            labeled_cmd_outputs: RefCell::new(Vec::new()),
            labeled_cmd_statuses: RefCell::new(Vec::new()),
            files: RefCell::new(HashMap::new()),
            git_has_changes_results: RefCell::new(Vec::new()),
        }
    }

    // ── Labeled response builders ────────────────────────────────────

    pub(crate) fn on_cmd(self, label: &str, output: &str) -> Self {
        self.labeled_cmd_outputs
            .borrow_mut()
            .push((label.to_string(), output.to_string()));
        self
    }

    pub(crate) fn on_cmd_success(self, label: &str, status: bool) -> Self {
        self.labeled_cmd_statuses
            .borrow_mut()
            .push((label.to_string(), status));
        self
    }

    pub(crate) fn with_file(self, path: &str, content: &str) -> Self {
        self.files
            .borrow_mut()
            .insert(PathBuf::from(path), content.to_string());
        self
    }

    pub(crate) fn with_git_has_changes(self, b: bool) -> Self {
        self.git_has_changes_results.borrow_mut().push(b);
        self
    }

    // ── Assertion helpers ────────────────────────────────────────────

    #[allow(dead_code)]
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.borrow().clone()
    }

    /// Panics with a listing of all recorded calls if no match is found.
    pub(crate) fn find_call(&self, pattern: &str) -> Call {
        let calls = self.calls.borrow();
        calls
            .iter()
            .find(|c| c.label().starts_with(pattern))
            .unwrap_or_else(|| {
                panic!(
                    "no call with label starting with `{pattern}`\nrecorded calls:\n{}",
                    format_call_list(&calls)
                )
            })
            .clone()
    }

    #[allow(dead_code)]
    pub(crate) fn assert_called(&self, pattern: &str) {
        let calls = self.calls.borrow();
        assert!(
            calls.iter().any(|c| c.label().starts_with(pattern)),
            "expected a call matching `{pattern}` but none found\nrecorded calls:\n{}",
            format_call_list(&calls)
        );
    }

    #[allow(dead_code)]
    pub(crate) fn assert_not_called(&self, pattern: &str) {
        let calls = self.calls.borrow();
        assert!(
            !calls.iter().any(|c| c.label().starts_with(pattern)),
            "expected no call matching `{pattern}` but found one\nrecorded calls:\n{}",
            format_call_list(&calls)
        );
    }

    /// Checks order and count. Each entry is matched via `starts_with` on the call label.
    pub(crate) fn assert_sequence(&self, expected: &[&str]) {
        let calls = self.calls.borrow();
        assert_eq!(
            calls.len(),
            expected.len(),
            "expected {} calls but got {}\nrecorded calls:\n{}",
            expected.len(),
            calls.len(),
            format_call_list(&calls)
        );
        for (i, (call, pattern)) in calls.iter().zip(expected.iter()).enumerate() {
            let label = call.label();
            assert!(
                label.starts_with(pattern),
                "call[{i}]: expected label starting with `{pattern}`, got `{label}`\nfull sequence:\n{}",
                format_call_list(&calls)
            );
        }
    }
}

fn format_call_list(calls: &[Call]) -> String {
    calls
        .iter()
        .enumerate()
        .map(|(i, c)| format!("  [{i}] {}", c.label()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn take_labeled<T>(entries: &mut Vec<(String, T)>, cmd_str: &str) -> Option<T> {
    let idx = entries
        .iter()
        .position(|(label, _)| cmd_str.starts_with(label.as_str()))?;
    Some(entries.remove(idx).1)
}

// ── Ops implementation ───────────────────────────────────────────────────

impl Ops for MockOps {
    fn run_cmd(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<String> {
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.calls.borrow_mut().push(Call::RunCmd {
            program: program.to_string(),
            args: args_owned.clone(),
            cwd: cwd.map(PathBuf::from),
        });
        let cmd_str = cmd_string(program, &args_owned);
        let output =
            take_labeled(&mut self.labeled_cmd_outputs.borrow_mut(), &cmd_str).unwrap_or_default();
        Ok(output)
    }

    fn cmd_success(&self, program: &str, args: &[&str]) -> Result<bool> {
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.calls.borrow_mut().push(Call::CmdSuccess {
            program: program.to_string(),
            args: args_owned.clone(),
        });
        let cmd_str = cmd_string(program, &args_owned);
        let status =
            take_labeled(&mut self.labeled_cmd_statuses.borrow_mut(), &cmd_str).unwrap_or(false);
        Ok(status)
    }

    fn read_file(&self, path: &Path) -> Result<String> {
        self.calls.borrow_mut().push(Call::ReadFile {
            path: path.to_path_buf(),
        });
        self.files
            .borrow()
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("mock: file not found: {}", path.display()))
    }

    fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::WriteFile {
            path: path.to_path_buf(),
            content: content.to_string(),
        });
        Ok(())
    }

    fn git_checkout(&self, branch: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GitCheckout {
            branch: branch.to_string(),
        });
        Ok(())
    }

    fn git_has_changes(&self, paths: &[&str]) -> Result<bool> {
        self.calls.borrow_mut().push(Call::GitHasChanges {
            paths: paths.iter().map(|s| s.to_string()).collect(),
        });
        let mut results = self.git_has_changes_results.borrow_mut();
        Ok(if results.is_empty() {
            false
        } else {
            results.remove(0)
        })
    }

    fn commit_and_push(&self, branch: &str, message: &str, paths: &[&str]) -> Result<()> {
        self.calls.borrow_mut().push(Call::CommitAndPush {
            branch: branch.to_string(),
            message: message.to_string(),
            paths: paths.iter().map(|s| s.to_string()).collect(),
        });
        Ok(())
    }

    fn gha_output(&self, key: &str, value: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GhaOutput {
            key: key.to_string(),
            value: value.to_string(),
        });
        Ok(())
    }

    fn gha_output_multiline(&self, key: &str, value: &str) -> Result<()> {
        self.calls.borrow_mut().push(Call::GhaOutputMultiline {
            key: key.to_string(),
            value: value.to_string(),
        });
        Ok(())
    }
}
