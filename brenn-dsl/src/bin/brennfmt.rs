//! The `.brenn` formatter. The CLI surface — `--check`, `--in-place`, stdin,
//! exit codes — comes from `fltk-fmt-cli`; the format spec is baked into the
//! generated unparser this names.

fltk_fmt_cli::fltk_formatter_main! {
    about: "Format Brenn configuration files.",
    parser: brenn_dsl::parser::Parser,
    unparser: brenn_dsl::unparser::Unparser,
    parse: apply__parse_file,
    unparse: unparse_file,
}
