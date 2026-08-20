//! Grammar-development ergonomics: read a document and say what came out.
//!
//! Two subcommands, and the difference between them is the whole pipeline.
//! `parse` reads one file through the front end: a document it accepts can
//! still name things that do not exist. `check` compiles a tree from its root
//! and reports everything the compiler can validate today.

use std::path::PathBuf;
use std::process::ExitCode;

use brenn_dsl::diag::Diagnostic;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "brenn-dsl", about = "Brenn configuration language tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse and deserialize one file.
    Parse {
        /// The `.brenn` file to read.
        file: PathBuf,
        /// Print the deserialized model.
        #[arg(long)]
        dump: bool,
    },
    /// Compile a document from its root file: parse, resolve, derive, report.
    Check {
        /// The root `.brenn` file. Its directory is the module root.
        root: PathBuf,
        /// Print the derived configuration.
        #[arg(long)]
        dump: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Parse { file, dump } => match brenn_dsl::parse_file(&file) {
            Ok(model) => {
                if dump {
                    println!("{model:#?}");
                } else {
                    println!("{}: ok", file.display());
                }
                ExitCode::SUCCESS
            }
            Err(diagnostic) => {
                report(&diagnostic);
                ExitCode::FAILURE
            }
        },
        Command::Check { root, dump } => match brenn_dsl::compile(&root) {
            Ok(output) => {
                for warning in &output.warnings {
                    report(warning);
                }
                if dump {
                    println!("{:#?}", output.config);
                } else {
                    println!("{}: ok", root.display());
                }
                ExitCode::SUCCESS
            }
            Err(errors) => {
                for error in &errors {
                    report(error);
                }
                ExitCode::FAILURE
            }
        },
    }
}

/// One diagnostic and its secondary locations, on stderr.
fn report(diagnostic: &Diagnostic) {
    eprintln!("{diagnostic}");
    for (note, span) in &diagnostic.related {
        eprintln!("  {}", Diagnostic::related_line(note, span));
    }
}
