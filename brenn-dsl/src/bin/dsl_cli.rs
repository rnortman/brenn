//! Grammar-development ergonomics: parse a file and say what came out.
//!
//! The subcommand is `parse`, not `check`, because nothing here resolves
//! references — a document this accepts can still name things that do not
//! exist.

use std::path::PathBuf;
use std::process::ExitCode;

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
                eprintln!("{diagnostic}");
                for (note, span) in &diagnostic.related {
                    eprintln!("  {note}: {span:?}");
                }
                ExitCode::FAILURE
            }
        },
    }
}
