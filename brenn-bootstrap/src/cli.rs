use std::path::PathBuf;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "brenn", about = "Brenn application server")]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Path to the mounts document: the `.brenn` file declaring which
    /// installed trees the host may read. Every root a server uses — the
    /// packaged modules `use @<name>::…` resolves against, the component
    /// packages a consumer loads from, the surface asset trees a page is
    /// served out of — is derived from it, so this is the whole of the
    /// server's environment-fact surface. Absent means zero mounts: a dev
    /// server with no components, no surfaces and no packaged vocabulary.
    ///
    /// Global so that the `mounts` subcommand, which an installer runs to read
    /// the operator's declaration, spells it after the subcommand the way the
    /// unit spells it before `serve`.
    #[arg(long, value_name = "FILE", global = true)]
    pub mounts: Option<PathBuf>,

    /// Directory holding the packaged component modules `use @<name>::…`
    /// imports resolve against. A **check-only** flag: it is the workstation
    /// form, for certifying a document against a source checkout that holds no
    /// mounts. A server derives its module roots from `--mounts` and refuses
    /// this flag.
    /// Repeatable; a module must be under exactly one of them.
    #[arg(long, value_name = "DIR")]
    pub modules: Vec<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the web server (default if no subcommand given).
    Serve,
    /// Generate an invite code and print it to stdout.
    Invite,
    /// List what the mounts document declares and what each declaration looks
    /// like on disk: one line per mount, `name<TAB>declared path<TAB>status`.
    /// Exits 0 whenever the document itself is a valid mounts document, even
    /// when a declared mount is not installed yet — an installer needs to read
    /// the declaration of the mount it is about to create.
    Mounts,
    /// Print the retained body of `brenn:config.status` — the outcome of this
    /// host's last boot or reload — as JSON on stdout. Exits 0 with a body, 2
    /// when the status channel holds none, 1 when the database cannot be read.
    /// Read-only: it runs while the server holds the store.
    ConfigStatus {
        /// The sqlite store the server was started against.
        #[arg(long, value_name = "PATH")]
        db: PathBuf,
    },
    /// Compare two `.brenn` config documents as configurations, not as
    /// documents. Exits 0 when they are the same config, 1 with a unified diff
    /// when they are not.
    ConfigDiff { a: PathBuf, b: PathBuf },
    /// Validate a `.brenn` config document the way the server loads it: parsed,
    /// resolved, derived and lowered. Exits 0 when the file would load, 1 with
    /// the diagnostics when it would not.
    /// Environment facts are not checked — container home directories, the
    /// integration registry and the runtime dir are the boot's business — so
    /// `ok` means the file is a config, not that it will boot on every host.
    ConfigCheck { file: PathBuf },
}

impl Commands {
    /// The subcommand as the operator spelled it, for a refusal to quote.
    fn name(&self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::Invite => "invite",
            Self::Mounts => "mounts",
            Self::ConfigStatus { .. } => "config-status",
            Self::ConfigDiff { .. } => "config-diff",
            Self::ConfigCheck { .. } => "config-check",
        }
    }

    /// Whether this subcommand reads module roots off a workstation checkout
    /// rather than off an install. Only the two config tools do; everything
    /// else is a host operation whose roots are the mounts'.
    fn takes_modules(&self) -> bool {
        matches!(self, Self::ConfigDiff { .. } | Self::ConfigCheck { .. })
    }
}

impl Cli {
    /// The rules clap cannot state.
    ///
    /// Two flag families name module roots and exactly one of them applies to
    /// any invocation: `--mounts` is what a host is installed as, `--modules`
    /// is what a workstation checks against. Clap's `conflicts_with` names
    /// arguments and not subcommands, and `args_conflicts_with_subcommands`
    /// would take `--config` down with it, so the rule is a post-parse check
    /// that produces the same clap-formatted error and the same exit code an
    /// argument conflict does.
    pub fn validate(&self) -> Result<(), clap::Error> {
        let command = self.command.as_ref().unwrap_or(&Commands::Serve);
        if !self.modules.is_empty() && !command.takes_modules() {
            return Err(Self::conflict(format!(
                "`{}` does not take --modules: a host's module roots come from --mounts, \
                 so that a bundle installed after boot is one reload away rather than one \
                 unit edit and one restart. --modules is the workstation form, and only \
                 `config-check` and `config-diff` take it",
                command.name(),
            )));
        }
        if !self.modules.is_empty() && self.mounts.is_some() {
            return Err(Self::conflict(
                "--mounts and --modules both name module roots: pass --mounts to check \
                 against an installed tree, or --modules to check against a source \
                 checkout, never both"
                    .to_string(),
            ));
        }
        if matches!(command, Commands::Mounts) && self.mounts.is_none() {
            return Err(Self::command_error(
                ErrorKind::MissingRequiredArgument,
                "`mounts` reads the mounts document, so it needs --mounts <FILE>".to_string(),
            ));
        }
        Ok(())
    }

    fn conflict(message: String) -> clap::Error {
        Self::command_error(ErrorKind::ArgumentConflict, message)
    }

    fn command_error(kind: ErrorKind, message: String) -> clap::Error {
        Self::command().error(kind, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--modules` is declared on the root parser and is not `global`, so it
    /// parses before the subcommand and nowhere else. The operator-facing
    /// invocation that certifies a config on the host before a bounce is
    /// spelled that way, so which orderings parse is a contract and not a
    /// convenience: making the flag global, or moving it onto the subcommands,
    /// breaks the last gate before a restart while every build stays green.
    #[test]
    fn the_module_root_is_named_before_the_subcommand_and_not_after_it() {
        let cli = Cli::try_parse_from(["brenn", "--modules", "/srv/modules", "config-check", "x"])
            .expect("the flag precedes the subcommand");
        assert_eq!(cli.modules, [PathBuf::from("/srv/modules")]);
        cli.validate()
            .expect("config-check takes the workstation form");
        let Some(Commands::ConfigCheck { file }) = cli.command else {
            panic!("the subcommand still parses");
        };
        assert_eq!(file, PathBuf::from("x"));

        assert!(
            Cli::try_parse_from(["brenn", "config-check", "--modules", "/srv/modules", "x"])
                .is_err(),
            "a subcommand of its own does not take the flag"
        );
    }

    /// The unit spells `--mounts` before `serve`; the installer spells it after
    /// `mounts`. Both are contracts, which is what the flag is global for.
    #[test]
    fn the_mounts_file_parses_on_either_side_of_the_subcommand() {
        let before = Cli::try_parse_from(["brenn", "--mounts", "/etc/mounts.brenn", "serve"])
            .expect("the unit's form parses");
        assert_eq!(before.mounts, Some(PathBuf::from("/etc/mounts.brenn")));
        before.validate().expect("serve takes the mounts file");

        let after = Cli::try_parse_from(["brenn", "mounts", "--mounts", "/etc/mounts.brenn"])
            .expect("the installer's form parses");
        assert_eq!(after.mounts, Some(PathBuf::from("/etc/mounts.brenn")));
        after.validate().expect("mounts takes the mounts file");
    }

    /// The retired flags are parse errors, not silently-ignored words: a unit
    /// carrying them must fail the installer's pre-stop argv check rather than
    /// boot a server serving nothing.
    #[test]
    fn serve_no_longer_takes_the_components_and_surface_roots() {
        assert!(
            Cli::try_parse_from(["brenn", "serve", "--components", "/srv/components"]).is_err(),
            "the components root comes from a mount"
        );
        assert!(
            Cli::try_parse_from(["brenn", "serve", "--surface", "/srv/surface"]).is_err(),
            "the surface root comes from a mount"
        );
    }

    /// A server's module roots are the mounts'. The refusal is clap-shaped so
    /// the exit code and the rendering match every other bad command line.
    #[test]
    fn a_server_refuses_the_workstation_module_flag() {
        for argv in [
            vec!["brenn", "--modules", "/srv/modules", "serve"],
            vec!["brenn", "--modules", "/srv/modules", "invite"],
            vec![
                "brenn",
                "--modules",
                "/srv/modules",
                "--mounts",
                "/etc/mounts.brenn",
                "mounts",
            ],
            // No subcommand at all is `serve`.
            vec!["brenn", "--modules", "/srv/modules"],
        ] {
            let cli = Cli::try_parse_from(&argv).expect("it parses; it does not validate");
            let error = cli
                .validate()
                .expect_err(&format!("{argv:?} names module roots a host derives"));
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
        }
    }

    /// Either form of module root, never both: a check run against two module
    /// universes is not a real operation.
    #[test]
    fn the_two_module_root_forms_are_exclusive_on_the_config_tools() {
        let cli = Cli::try_parse_from([
            "brenn",
            "--mounts",
            "/etc/mounts.brenn",
            "--modules",
            "/srv/modules",
            "config-check",
            "x",
        ])
        .expect("it parses");
        let error = cli.validate().expect_err("two universes");
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);

        Cli::try_parse_from([
            "brenn",
            "--mounts",
            "/etc/mounts.brenn",
            "config-check",
            "x",
        ])
        .expect("it parses")
        .validate()
        .expect("the installed form checks");
        Cli::try_parse_from([
            "brenn",
            "--modules",
            "/srv/modules",
            "config-diff",
            "a",
            "b",
        ])
        .expect("it parses")
        .validate()
        .expect("the workstation form checks");
        Cli::try_parse_from(["brenn", "config-check", "x"])
            .expect("it parses")
            .validate()
            .expect("no module root at all is a document that imports nothing");
    }

    /// `mounts` with nothing to read is an operator error, not an empty
    /// listing: the empty listing is what "no mount is declared" looks like,
    /// and a forgotten flag must not be mistaken for it.
    #[test]
    fn the_mounts_listing_needs_a_mounts_file() {
        let error = Cli::try_parse_from(["brenn", "mounts"])
            .expect("it parses")
            .validate()
            .expect_err("there is nothing to list");
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    /// One flag per installed release, in the order written. Parsing keeps the
    /// order because the refusals that list the roots quote it.
    #[test]
    fn the_module_root_flag_repeats_once_per_checked_tree() {
        let cli = Cli::try_parse_from([
            "brenn",
            "--modules",
            "/srv/brenn/modules",
            "--modules",
            "/srv/bundle/modules",
            "config-check",
            "x",
        ])
        .expect("every flag repeats");
        assert_eq!(
            cli.modules,
            [
                PathBuf::from("/srv/brenn/modules"),
                PathBuf::from("/srv/bundle/modules")
            ]
        );

        let cli = Cli::try_parse_from(["brenn", "serve"]).expect("no flag is required");
        assert!(cli.modules.is_empty());
        assert!(cli.mounts.is_none());
        cli.validate().expect("a dev server declares no mount");
    }
}
