//! The `.brenn` formatter. The CLI surface — `--check`, `--in-place`, stdin,
//! exit codes — comes from `fltk-fmt-cli`; the format spec is baked into the
//! generated unparser this names.
//!
//! TODO(dsl-fmt-rawstring-indent): an indented multi-line raw string has its
//! continuation lines re-indented into its own value; the fix is in the
//! formatting core, not here.
//!
//! TODO(dsl-fmt-orphan-terminator): a tail-block statement followed by a block
//! statement gains one leading space, and so does a body-less `section`
//! followed by a blank line. Layout only, and its own fixed point.

fltk_fmt_cli::fltk_formatter_main! {
    about: "Format Brenn configuration files.",
    parser: brenn_dsl::parser::Parser,
    unparser: brenn_dsl::unparser::Unparser,
    parse: apply__parse_file,
    unparse: unparse_file,
}
