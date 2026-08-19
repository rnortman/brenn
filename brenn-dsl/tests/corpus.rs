//! Corpus suite: what the grammar accepts, what it refuses, and what the model
//! makes of it.

use fltk_cst_core::Span;

use brenn_dsl::model::{
    BraceEscape, ConstDef, FStrPart, File, InstBody, Item, PathSeg, StrPart, Value,
    section_kindword,
};
use brenn_dsl::{parse_file, parse_str};

mod support;

use support::corpus_text as read;

/// The `const` declaration named `name`, which the fixture must carry.
fn constant<'a>(file: &'a File, name: &str) -> &'a ConstDef {
    file.consts()
        .find(|constant| constant.name.value() == name)
        .unwrap_or_else(|| panic!("the corpus declares a constant named {name}"))
}

#[test]
fn the_lexical_corpus_deserializes() {
    let src = read("lexical.brenn");
    let file = parse_str(&src, "lexical.brenn").expect("the corpus must parse");

    assert_eq!(file.uses.len(), 2);
    assert_eq!(file.uses[0].path.head.value(), "wiring");
    assert!(!file.uses[0].glob);
    assert!(file.uses[1].glob);
    let [PathSeg::Module(segment)] = &file.uses[1].path.segs[..] else {
        panic!("one `::`-qualified segment");
    };
    assert_eq!(segment.name.value(), "bob");
}

/// An import tolerates whitespace before its terminator, the way every other
/// statement does, and tolerates it before the glob for the same reason: both
/// sit at the one optional-whitespace position the rule has.
#[test]
fn an_import_tolerates_whitespace_before_its_terminator_and_its_glob() {
    let file = parse_str("use a::b ;\n", "t.brenn").expect("a space before the `;`");
    assert!(!file.uses[0].glob);

    let file = parse_str("use a ::*;\n", "t.brenn").expect("a space before the glob");
    assert!(file.uses[0].glob);
    assert!(file.uses[0].path.segs.is_empty());
}

#[test]
fn a_doc_comment_reaches_the_model_and_its_lines_stay_separate() {
    let src = read("lexical.brenn");
    let file = parse_str(&src, "lexical.brenn").expect("the corpus must parse");

    let Item::ConstDef(components_dir) = file.items[0].value() else {
        panic!("the first item is a constant");
    };
    let doc = components_dir
        .doc
        .as_ref()
        .expect("the constant carries a doc comment");
    assert_eq!(doc.lines.len(), 2);
    assert_eq!(
        doc.lines[0].value(),
        " Where component artifacts live on this host."
    );
}

#[test]
fn a_line_comment_is_trivia_and_a_doc_comment_is_not() {
    let file = parse_str("// just trivia\nconst a = 1;\n", "t.brenn").expect("a parse");
    assert_eq!(file.items.len(), 1);
    let Item::ConstDef(constant) = file.items[0].value() else {
        panic!("a constant");
    };
    assert!(constant.doc.is_none(), "a `//` comment does not attach");
}

/// The fourth slash is content: `////` is a doc comment whose text starts with
/// a slash, not a line comment.
#[test]
fn four_slashes_are_a_doc_comment() {
    let file = parse_str("////text\nconst a = 1;\n", "t.brenn").expect("a parse");
    let Item::ConstDef(constant) = file.items[0].value() else {
        panic!("a constant");
    };
    let doc = constant.doc.as_ref().expect("a doc comment");
    assert_eq!(doc.lines[0].value(), "/text");
}

/// A `.brenn` file ends with a newline: a line comment's terminator is part of
/// the rule.
#[test]
fn a_trailing_line_comment_without_a_newline_is_refused() {
    parse_str("const a = 1;\n// no newline", "t.brenn").expect_err("the comment never terminates");
}

#[test]
fn a_float_is_not_read_as_an_integer_and_a_stranded_fraction() {
    let file = parse_str("const a = 1.0;\nconst b = 1;\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Item::ConstDef(b) = file.items[1].value() else {
        panic!("a constant");
    };
    assert!(matches!(a.value.value(), Value::Flt(_)));
    assert!(matches!(b.value.value(), Value::Int(_)));
}

/// A number reaches the model as its value, not as its lexeme: the sign, the
/// fraction and the exponent all have to survive the conversion.
#[test]
fn a_number_carries_its_value_and_not_its_text() {
    let src = read("lexical.brenn");
    let file = parse_str(&src, "lexical.brenn").expect("the corpus must parse");

    let Value::Int(retries) = constant(&file, "retries").value.value() else {
        panic!("an integer");
    };
    assert_eq!(*retries.value(), -3);

    let Value::Flt(ratio) = constant(&file, "ratio").value.value() else {
        panic!("a float");
    };
    assert_eq!(*ratio.value(), 1.5);

    let Value::Flt(scaled) = constant(&file, "scaled").value.value() else {
        panic!("a float");
    };
    assert_eq!(*scaled.value(), -2500.0);
}

/// `integer` admits digits an `i64` cannot hold. The conversion refuses them
/// where they were written rather than wrapping.
#[test]
fn an_integer_past_i64_is_a_positioned_refusal() {
    let error = parse_str("const a = 99999999999999999999;\n", "overflow.brenn")
        .expect_err("the literal does not fit");
    assert!(error.message.contains("i64"), "{}", error.message);
    assert_eq!(error.line_col(), Some((1, 11)));
}

/// A raw string reaches the model as its interior text: no delimiters, no
/// escape decoding, and the newline it spans intact.
#[test]
fn a_raw_string_carries_its_undecoded_interior() {
    let src = read("lexical.brenn");
    let file = parse_str(&src, "lexical.brenn").expect("the corpus must parse");

    let Value::Raw(notes) = constant(&file, "notes").value.value() else {
        panic!("a raw string");
    };
    assert_eq!(
        notes.value(),
        "raw text, no escapes: \\n stays two characters\nand it spans lines"
    );
}

/// `true` and `false` are reserved in value position: the boolean alternative
/// is ordered before the reference one, so a constant cannot be named `true`
/// and then referenced.
#[test]
fn a_boolean_beats_a_reference() {
    let file = parse_str("const a = true;\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Value::Bool(flag) = a.value.value() else {
        panic!("a boolean, not a reference");
    };
    assert!(*flag.value());
}

/// An identifier followed by a payload is a matcher; an identifier alone is a
/// reference.
#[test]
fn a_matcher_beats_a_reference() {
    let file = parse_str(
        "const a = exact \"brenn:alice.in\";\nconst b = alice;\n",
        "t.brenn",
    )
    .expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Item::ConstDef(b) = file.items[1].value() else {
        panic!("a constant");
    };
    let Value::M(matcher) = a.value.value() else {
        panic!("a matcher");
    };
    assert_eq!(matcher.kind.value(), "exact");
    assert!(matches!(b.value.value(), Value::Ref(_)));
}

#[test]
fn a_plain_string_carries_its_escapes_undecoded() {
    let file = parse_str("const a = \"x\\ny\";\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Value::Str(literal) = a.value.value() else {
        panic!("a string");
    };
    assert_eq!(literal.parts.len(), 3);
    assert!(matches!(&literal.parts[1], StrPart::Esc(escape) if escape.value() == "n"));
}

#[test]
fn an_f_string_separates_interpolations_from_literal_braces() {
    let file = parse_str("const a = f\"{{x}} {alice.desk}\";\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Value::Fstr(literal) = a.value.value() else {
        panic!("an f-string");
    };
    let [
        FStrPart::Brace(open),
        FStrPart::Frag(inside),
        FStrPart::Brace(close),
        FStrPart::Frag(space),
        FStrPart::Interp(path),
    ] = &literal.parts[..]
    else {
        panic!("two literal braces around a fragment, then an interpolation");
    };
    assert_eq!(open.value(), &BraceEscape::Open);
    assert_eq!(close.value(), &BraceEscape::Close);
    assert_eq!(inside.value(), "x");
    assert_eq!(space.value(), " ");
    assert_eq!(path.head.value(), "alice");
}

/// The two segment separators are different things — `.` reaches inside an
/// instantiated assembly, `::` names a module — and they carry the same payload
/// shape, so only the variant tells them apart.
#[test]
fn a_path_records_which_separator_introduced_each_segment() {
    let file = parse_str("const a = alice.desk::x.y;\n", "t.brenn").expect("a parse");
    let Value::Ref(path) = constant(&file, "a").value.value() else {
        panic!("a reference");
    };
    assert_eq!(path.head.value(), "alice");
    let [PathSeg::Inst(desk), PathSeg::Module(x), PathSeg::Inst(y)] = &path.segs[..] else {
        panic!("a dotted, then a module, then a dotted segment");
    };
    assert_eq!(
        [desk.name.value(), x.name.value(), y.name.value()],
        ["desk", "x", "y"]
    );
}

/// A plain string does not interpolate: the braces are ordinary characters.
#[test]
fn a_plain_string_does_not_interpolate() {
    let file = parse_str("const a = \"{alice}\";\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Value::Str(literal) = a.value.value() else {
        panic!("a string");
    };
    assert_eq!(literal.parts.len(), 1);
    assert!(matches!(&literal.parts[0], StrPart::Frag(fragment) if fragment.value() == "{alice}"));
}

/// Raw content cannot end in a quote — the ladder that terminates the match
/// without lookaround leaves the trailing quote outside the content, and the
/// parse fails on the residue. A plain string with escapes is the way to write
/// it.
#[test]
fn raw_string_content_cannot_end_in_a_quote() {
    parse_str("const a = \"\"\"ends in a quote\"\"\"\";\n", "t.brenn")
        .expect_err("the trailing quote is residue");
    parse_str(
        "const a = \"\"\"has \"interior\" quotes\"\"\";\n",
        "t.brenn",
    )
    .expect("interior quotes ride the ladder");
}

/// A duplicated key is refused at the second entry and cites the first: "you
/// wrote it here, and already here" is the whole value of the diagnostic.
#[test]
fn a_duplicate_key_in_one_body_is_refused_and_cites_the_first_entry() {
    let error = parse_str("const a = { x = 1, x = 2 };\n", "t.brenn")
        .expect_err("the second `x` has no home");
    assert!(
        error.message.contains("duplicate"),
        "the refusal names what it is: {error}"
    );
    assert_eq!(error.line_col(), Some((1, 20)));

    let [(note, first)] = &error.related[..] else {
        panic!("one related location: the first entry");
    };
    assert!(note.contains("previously defined"), "{note}");
    assert_eq!(line_col(first), Some((1, 13)));
}

/// `line:col` of a span, one-based, the way `Diagnostic::line_col` reports its
/// own.
fn line_col(span: &Span) -> Option<(i64, i64)> {
    span.line_col_inner()
        .map(|position| (position.line + 1, position.col + 1))
}

/// A syntax error has no span of its own: the front end renders position into
/// the message and the diagnostic carries only the file.
#[test]
fn a_syntax_error_names_its_file_without_a_position() {
    let error = parse_str("const a = ;\n", "alice.brenn").expect_err("a value is required");
    assert_eq!(error.file, "alice.brenn");
    assert_eq!(error.line_col(), None);
    assert_eq!(
        format!("{error}"),
        format!("alice.brenn: {}", error.message)
    );
}

/// A failure raised after the parse is positioned at the offending token, and
/// `Display` renders that position — the crate's central promise, asserted at
/// the entry point every caller uses.
#[test]
fn a_deserialize_failure_through_parse_str_is_positioned() {
    let error = parse_str(
        "component Alice {\n    abi = dom;\n    abi = processor;\n}\n",
        "alice.brenn",
    )
    .expect_err("the second `abi` has no home");
    assert_eq!(error.file, "alice.brenn");
    assert_eq!(error.line_col(), Some((3, 5)));
    assert!(format!("{error}").contains("alice.brenn:3:5:"), "{error}");
}

/// A body's entries are readable in the order they were written, which is what
/// lets a diagnostic citing two of them cite them in that order.
///
/// An instance body is the untyped one: which vocabulary applies depends on the
/// class, so its entries stay a map until resolution says.
#[test]
fn an_attr_map_keeps_source_order_and_reports_an_empty_body() {
    let file = parse_str(
        "new a: Alice {\n    zeta = 1;\n    alpha = 2;\n    mid = 3;\n}\n",
        "t.brenn",
    )
    .expect("a parse");
    let body = instance_body(&file);
    let keys: Vec<&str> = body
        .attrs
        .entries()
        .iter()
        .map(|(key, _)| key.as_str())
        .collect();
    assert_eq!(keys, ["zeta", "alpha", "mid"]);
    assert!(!body.attrs.is_empty());

    let file = parse_str("new a: Alice {\n}\n", "t.brenn").expect("a parse");
    let body = instance_body(&file);
    assert!(body.attrs.is_empty());
    assert_eq!(body.attrs.len(), 0);
}

/// The body of the document's one instantiation.
fn instance_body(file: &File) -> &InstBody {
    file.instantiations()
        .next()
        .expect("one instantiation")
        .body
        .as_ref()
        .expect("it was written with a body")
}

/// Pathological nesting stops at the generated entry point's depth limit, and
/// the refusal says so rather than arriving as a stack overflow.
#[test]
fn nesting_past_the_depth_limit_is_a_clean_refusal() {
    let depth = 4_000;
    let src = format!("const a = {}{};\n", "[".repeat(depth), "]".repeat(depth));
    let error = parse_str(&src, "deep.brenn").expect_err("the nesting is past the limit");
    assert!(error.message.contains("depth limit"), "{error}");
    assert_eq!(error.file, "deep.brenn");
}

#[test]
fn a_generic_section_is_held_as_its_own_subtree() {
    let src = read("lexical.brenn");
    let file = parse_str(&src, "lexical.brenn").expect("the corpus must parse");
    let sections: Vec<_> = file.sections().collect();
    assert_eq!(sections.len(), 1, "one top-level section, held un-walked");
    assert_eq!(section_kindword(sections[0]).0, "server");
}

/// The on-disk entry point, which is what the CLI calls: the same bytes give
/// the same model, and the diagnostic names the path it was handed.
#[test]
fn parse_file_reads_the_path_it_is_given() {
    let src = read("lexical.brenn");
    let path = std::env::temp_dir().join("brenn-dsl-parse-file.brenn");
    std::fs::write(&path, &src).expect("a writable temp dir");

    let from_disk = parse_file(&path).expect("the corpus parses");
    let in_memory = parse_str(&src, &path.display().to_string()).expect("the corpus parses");
    assert_eq!(from_disk.uses, in_memory.uses);
    assert_eq!(from_disk.items.len(), in_memory.items.len());
    std::fs::remove_file(&path).expect("the fixture was written");
}

#[test]
fn parse_file_reports_an_unreadable_path_without_a_position() {
    let path = std::env::temp_dir().join("brenn-dsl-no-such-file.brenn");
    let error = parse_file(&path).expect_err("the file does not exist");
    assert_eq!(error.file, path.display().to_string());
    assert_eq!(error.line_col(), None);
    assert_eq!(
        format!("{error}"),
        format!("{}: {}", path.display(), error.message)
    );
}

#[test]
fn a_unicode_value_survives_the_crossing() {
    let file = parse_str("const a = \"snowman \u{2603}\";\n", "t.brenn").expect("a parse");
    let Item::ConstDef(a) = file.items[0].value() else {
        panic!("a constant");
    };
    let Value::Str(literal) = a.value.value() else {
        panic!("a string");
    };
    assert!(
        matches!(&literal.parts[0], StrPart::Frag(fragment) if fragment.value().contains('\u{2603}'))
    );
}

/// Round trip: the canonical form of the corpus deserializes to the same model
/// the source did. Spans differ and do not take part in equality; nothing else
/// may.
#[test]
fn formatting_preserves_the_model() {
    let source = parse_str(&read("lexical.brenn"), "lexical.brenn").expect("the corpus parses");
    let canonical = parse_str(&read("lexical.canonical.brenn"), "lexical.canonical.brenn")
        .expect("the canonical form parses");

    assert_eq!(source.uses, canonical.uses);
    fn constants(file: &File) -> Vec<&ConstDef> {
        file.consts().collect()
    }
    assert_eq!(constants(&source), constants(&canonical));

    // A held section compares by its whole subtree, layout included, which is
    // what formatting moves — so the comparison is over what the kindwords say.
    fn kindwords(file: &File) -> Vec<String> {
        file.sections()
            .map(|node| section_kindword(node).0)
            .collect()
    }
    assert_eq!(kindwords(&source), kindwords(&canonical));
    assert_eq!(kindwords(&source), ["server"]);
}
