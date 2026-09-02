//! Grammar-development ergonomics: read a document and say what came out.
//!
//! Four subcommands. `parse` reads one file through the front end: a document
//! it accepts can still name things that do not exist. `check` compiles a tree
//! from its root and reports everything the compiler can validate today.
//! `scaffold` reads one component specification and writes the Rust module its
//! guest crate compiles against. `grant-parity` holds one specification's
//! `requires` list equal to what its built artifact imports.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use brenn_dsl::diag::render_all;
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
        /// The root `.brenn` file. Its directory is where `use` resolves from.
        root: PathBuf,
        /// A directory `use @<name>::…` imports resolve against. Repeatable:
        /// each installed release's module root is one `--modules`, and a
        /// module must be under exactly one of them.
        #[arg(long, value_name = "DIR")]
        modules: Vec<PathBuf>,
        /// Print the derived configuration.
        #[arg(long)]
        dump: bool,
    },
    /// Generate the guest module for one component specification.
    Scaffold {
        /// The `.brenn` specification to generate from.
        spec: PathBuf,
        /// Which component class to take, where the module declares more than
        /// one.
        #[arg(long, value_name = "NAME")]
        class: Option<String>,
        /// Where to write the generated Rust source.
        #[arg(short = 'o', long = "out", value_name = "FILE")]
        out: PathBuf,
    },
    /// Hold one specification's `requires` list equal to its artifact's
    /// imports.
    ///
    /// The import list arrives as a file of names rather than being scraped
    /// here, because scraping it is the build's job and the build already does
    /// it once for every artifact it packages. A second reader of `wasm-tools`
    /// output would be one more thing to keep in step, and the one that fell
    /// behind would find nothing to compare — which for a set equality reads as
    /// a specification requiring everything it should not.
    GrantParity {
        /// The `.brenn` specification the component's package carries.
        #[arg(long, value_name = "FILE")]
        spec: PathBuf,
        /// A file of the artifact's imported interface names, one per line.
        #[arg(long, value_name = "FILE")]
        imports: PathBuf,
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
                eprintln!("{}", diagnostic.render());
                ExitCode::FAILURE
            }
        },
        Command::Check {
            root,
            modules,
            dump,
        } => match brenn_dsl::compile(&brenn_dsl::DocumentInputs {
            root: root.clone(),
            module_roots: modules,
        }) {
            Ok(config) => {
                if dump {
                    println!("{config:#?}");
                } else {
                    println!("{}: ok", root.display());
                }
                ExitCode::SUCCESS
            }
            Err(errors) => {
                eprintln!("{}", render_all(&errors));
                ExitCode::FAILURE
            }
        },
        Command::Scaffold { spec, class, out } => scaffold(&spec, class.as_deref(), &out),
        Command::GrantParity { spec, imports } => grant_parity(&spec, &imports),
    }
}

/// Compare one specification against one artifact's import list.
///
/// Exit status is the whole interface: zero when the two agree, non-zero with a
/// rendered diagnostic when they do not.
fn grant_parity(spec: &Path, imports: &Path) -> ExitCode {
    let text = match std::fs::read_to_string(imports) {
        Ok(text) => text,
        Err(error) => {
            eprintln!("{}: {error}", imports.display());
            return ExitCode::FAILURE;
        }
    };
    let names: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    // An empty list is refused rather than compared. Every component imports
    // the types interface at minimum, so nothing legitimately scrapes to
    // nothing; an empty file means the scrape found no imports it could read,
    // and comparing against it would report every requirement as unimported
    // drift and every clean component as broken.
    if names.is_empty() {
        eprintln!(
            "{}: no imports listed. Every component imports at least the types interface, \
             so an empty list is a scrape that read nothing rather than an artifact that \
             imports nothing.",
            imports.display()
        );
        return ExitCode::FAILURE;
    }
    match brenn_dsl::grant_parity::check_file(spec, &names) {
        Ok(()) => ExitCode::SUCCESS,
        Err(diagnostic) => {
            eprintln!("{}", diagnostic.render());
            ExitCode::FAILURE
        }
    }
}

/// Generate one guest module, or report why the specification cannot produce
/// one.
///
/// The basename is what the generated header names, so the module says which
/// document it came from without carrying a build-tree path that means nothing
/// to a reader.
fn scaffold(spec: &Path, class: Option<&str>, out: &Path) -> ExitCode {
    let filename = spec.display().to_string();
    let basename = spec
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| filename.clone());
    let generated = brenn_dsl::parse_file(spec)
        .and_then(|file| brenn_dsl::scaffold::generate(&file, class, &basename, &filename));
    match generated {
        Ok(source) => {
            // Nothing downstream can tell a truncated module from a short one,
            // so a write that does not land is a failure here rather than a
            // compile error somewhere else.
            match std::fs::write(out, source) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("{}: {error}", out.display());
                    ExitCode::FAILURE
                }
            }
        }
        Err(diagnostic) => {
            eprintln!("{}", diagnostic.render());
            ExitCode::FAILURE
        }
    }
}
