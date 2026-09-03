use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "brenn", about = "Brenn application server")]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Directory holding the packaged component modules `use @<name>::…`
    /// imports resolve against. An environment fact, so it is named here and
    /// never in the document: the same document checks on a workstation against
    /// a source checkout and boots on a host against the installed tree.
    /// Repeatable, one per installed release; a module must be under exactly
    /// one of them.
    #[arg(long, value_name = "DIR")]
    pub modules: Vec<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the web server (default if no subcommand given).
    Serve {
        /// Directory holding the installed component packages, one directory
        /// per package, named by the package. A boot fact only: config
        /// validation never resolves artifacts, so a document checks without
        /// components installed. Repeatable, one per installed release; a
        /// package must be under exactly one of them.
        #[arg(long, value_name = "DIR")]
        components: Vec<PathBuf>,

        /// Directory holding an installed surface asset tree, served under
        /// `/surface-static`. An artifact fact, so it is named here and never
        /// in the document. Repeatable, one per installed release; exactly one
        /// root carries the kernel module pair, and a kind must be under
        /// exactly one of them.
        #[arg(long, value_name = "DIR")]
        surface: Vec<PathBuf>,
    },
    /// Generate an invite code and print it to stdout.
    Invite,
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

/// The install-root lists a *server* boot was given, as one value.
///
/// Two lists: the component packages a consumer loads from, and the surface
/// asset trees a page is served out of. They are named on the command line
/// rather than in the document because where a release is installed is an
/// environment fact, and they are one value because named fields are what keeps
/// two lists of the same type from being handed over in each other's place.
///
/// The module roots are the third root type and are deliberately not here:
/// they are resolved before a server exists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstallRoots {
    pub components: Vec<PathBuf>,
    pub surface: Vec<PathBuf>,
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

    #[test]
    fn the_components_root_is_a_serve_flag_and_not_a_config_tool_flag() {
        let cli = Cli::try_parse_from(["brenn", "serve", "--components", "/srv/components"])
            .expect("serve takes the flag");
        let Some(Commands::Serve { components, .. }) = cli.command else {
            panic!("the subcommand parses");
        };
        assert_eq!(components, [PathBuf::from("/srv/components")]);

        assert!(
            Cli::try_parse_from([
                "brenn",
                "config-check",
                "--components",
                "/srv/components",
                "x"
            ])
            .is_err(),
            "config-check does not take the flag"
        );
        assert!(
            Cli::try_parse_from(["brenn", "--components", "/srv/components", "serve"]).is_err(),
            "the flag belongs to the subcommand, not the root parser"
        );
    }

    /// The surface asset tree is an artifact fact, so it sits beside
    /// `--components` on `serve` and not in the config document. The config
    /// tools never resolve artifacts, so they do not take it.
    #[test]
    fn the_surface_root_is_a_serve_flag_and_not_a_config_tool_flag() {
        let cli = Cli::try_parse_from(["brenn", "serve", "--surface", "/srv/surface"])
            .expect("serve takes the flag");
        let Some(Commands::Serve { surface, .. }) = cli.command else {
            panic!("the subcommand parses");
        };
        assert_eq!(surface, [PathBuf::from("/srv/surface")]);

        assert!(
            Cli::try_parse_from(["brenn", "config-check", "--surface", "/srv/surface", "x"])
                .is_err(),
            "config-check does not take the flag"
        );
        assert!(
            Cli::try_parse_from(["brenn", "--surface", "/srv/surface", "serve"]).is_err(),
            "the flag belongs to the subcommand, not the root parser"
        );
    }

    /// One flag per installed release, in the order written: brenn's roots and
    /// then each bundle's, or whatever order the unit spells. Parsing keeps the
    /// order because the refusals that list the roots quote it.
    #[test]
    fn each_root_flag_repeats_once_per_installed_release() {
        let cli = Cli::try_parse_from([
            "brenn",
            "--modules",
            "/srv/brenn/modules",
            "--modules",
            "/srv/bundle/modules",
            "serve",
            "--components",
            "/srv/brenn/components",
            "--components",
            "/srv/bundle/components",
            "--surface",
            "/srv/brenn/surface",
            "--surface",
            "/srv/bundle/surface",
        ])
        .expect("every flag repeats");
        assert_eq!(
            cli.modules,
            [
                PathBuf::from("/srv/brenn/modules"),
                PathBuf::from("/srv/bundle/modules")
            ]
        );
        let Some(Commands::Serve {
            components,
            surface,
        }) = cli.command
        else {
            panic!("the subcommand parses");
        };
        assert_eq!(
            components,
            [
                PathBuf::from("/srv/brenn/components"),
                PathBuf::from("/srv/bundle/components")
            ]
        );
        assert_eq!(
            surface,
            [
                PathBuf::from("/srv/brenn/surface"),
                PathBuf::from("/srv/bundle/surface")
            ]
        );

        let cli = Cli::try_parse_from(["brenn", "serve"]).expect("no flag is required");
        assert!(cli.modules.is_empty());
        let Some(Commands::Serve {
            components,
            surface,
        }) = cli.command
        else {
            panic!("the subcommand parses");
        };
        assert!(components.is_empty());
        assert!(surface.is_empty());
    }
}
