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
    #[arg(long, value_name = "DIR")]
    pub modules: Option<PathBuf>,

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
        /// components installed.
        #[arg(long, value_name = "DIR")]
        components: Option<PathBuf>,
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

#[cfg(test)]
mod tests {
    use std::path::Path;

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
        assert_eq!(cli.modules.as_deref(), Some(Path::new("/srv/modules")));
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
        let Some(Commands::Serve { components }) = cli.command else {
            panic!("the subcommand parses");
        };
        assert_eq!(components.as_deref(), Some(Path::new("/srv/components")));

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
}
