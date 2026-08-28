//! The shipped config files at the repo root, checked as files.
//!
//! One claim lives here: each file is a config the runtime could take — it
//! loads, it sets the one field the messaging bootstrap requires
//! unconditionally, and both channel-tuning passes run clean over its channel
//! blocks.

use brenn_surface_schema::CONTROL_PLANE_VERSION;

use super::*;
use crate::config::brenn::check_config;

fn load_config_file(filename: &str) -> BrennConfig {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    check_config(&root.join(filename), Some(&root.join("config/specs")))
        .unwrap_or_else(|report| panic!("{filename} must load: {report}"))
}

// Full validation needs host-side paths to exist, so it runs only at server
// startup. This invariant needs none of those paths and catches a regression
// at `make check` time that only a live start would otherwise surface.
fn assert_config_file_messaging_invariant(filename: &str) {
    let config = load_config_file(filename);
    let public_url = config
        .server
        .public_url
        .as_deref()
        .unwrap_or_else(|| panic!("{filename} must set server.public_url; it is required"));
    assert!(
        !public_url.is_empty(),
        "{filename} sets an empty server.public_url; it must be a well-formed URL"
    );
    // Every depth a `channel` block owns is required, and nothing under
    // the block supplies one. Running both passes here catches a misconfigured
    // block at `make check` time rather than only at a live server start.
    crate::messaging::config::build_channel_entries(&config.channels, &config.messaging);
    crate::messaging::config::build_system_channel_tuning(&config.channels, &config.messaging);
    assert_surface_description_channels_declared(filename, &config);
}

/// Asserts that each surface declares the four description channels its slug
/// requires (`help`, `geometry`, `status`, `bindings`).
///
/// The first three are durable and uuid-pinned, so a stem typo refuses to
/// compile. The bindings channel is ephemeral and unpinned: a typo there
/// compiles clean, leaving declared depths on an address nothing opens.
fn assert_surface_description_channels_declared(filename: &str, config: &BrennConfig) {
    let prefix = &config.surface_description.prefix;
    let declared: Vec<&str> = config
        .channels
        .iter()
        .filter_map(|channel| channel.address.as_deref())
        .collect();
    for surface in &config.surfaces {
        let slug = &surface.slug;
        for address in [
            format!("brenn:{prefix}.surface.{slug}.help"),
            format!("brenn:{prefix}.surface.{slug}.geometry"),
            format!("brenn:{prefix}.surface.{slug}.status"),
            format!("ephemeral:{prefix}.surface.{slug}.bindings"),
        ] {
            assert!(
                declared.contains(&address.as_str()),
                "{filename} declares surface `{slug}` but no channel at `{address}`, \
                 which is where the runtime derives that surface's description channel"
            );
        }
    }
}

/// Every `brenn.surface.*` doctype tag in the shipped config states the control
/// plane's current version.
///
/// A tag is nominal — nothing resolves one to a schema type — and the agreement
/// pass compares tags only to each other, so a bump of
/// [`CONTROL_PLANE_VERSION`] would leave every spec asserting `@1` about a
/// contract that no longer exists and every check green. The version half of
/// the coupling is mechanical, so it is checked here; the shape half is what
/// the schema crate's own types are for.
///
/// The plane *name* is checked no more than the shape is: the schema crate
/// exports no list of plane names to compare against, so `brenn.surface.thmee@1`
/// passes here and is caught only by the agreement pass, once a second port
/// names the plane correctly.
#[test]
fn every_control_plane_doctype_tag_states_the_current_version() {
    let mut seen = 0;
    for (filename, source) in config_sources() {
        for (plane, version) in control_plane_tags(&filename, &source) {
            assert_eq!(
                version, CONTROL_PLANE_VERSION,
                "{filename}: `{PLANE_PREFIX}{plane}@{version}` names a control plane \
                 the running one is not"
            );
            seen += 1;
        }
    }
    assert!(
        seen > 0,
        "no control-plane doctype tag was found at all; this test is reading \
         the wrong tree, not passing"
    );
}

/// The doctype namespace the surface control plane's documents live in.
const PLANE_PREFIX: &str = "brenn.surface.";

/// Every control-plane doctype tag one file states, as plane name and version.
///
/// A tag is a whole quoted string in code, so the scan reads only the code half
/// of each line and stops at the closing quote: prose in a comment that mentions
/// a plane is not a tag, and a tag that forgot its `@version` is refused here
/// rather than satisfied by an unrelated `@` further down the file.
fn control_plane_tags<'a>(filename: &str, source: &'a str) -> Vec<(&'a str, u8)> {
    let mut tags = Vec::new();
    for line in source.lines() {
        let mut rest = code_of(line);
        while let Some(at) = rest.find(&format!("\"{PLANE_PREFIX}")) {
            rest = &rest[at + 1 + PLANE_PREFIX.len()..];
            let plane_len = rest
                .find(|character: char| !is_tag_character(character))
                .unwrap_or(rest.len());
            let (plane, tail) = rest.split_at(plane_len);
            let tail = tail.strip_prefix('@').unwrap_or_else(|| {
                panic!("{filename}: `{PLANE_PREFIX}{plane}` states no `@version`")
            });
            let digits: String = tail.chars().take_while(char::is_ascii_digit).collect();
            let version: u8 = digits.parse().unwrap_or_else(|error| {
                panic!("{filename}: `{PLANE_PREFIX}{plane}@{digits}` has no version: {error}")
            });
            rest = &tail[digits.len()..];
            assert!(
                rest.starts_with('"'),
                "{filename}: `{PLANE_PREFIX}{plane}@{digits}` runs on past its version; \
                 a doctype tag is a plane and a version and nothing else"
            );
            tags.push((plane, version));
        }
    }
    tags
}

/// One line with any trailing `//` comment cut off.
///
/// Quote parity decides where a comment starts, so a `//` inside a string — a
/// URL — is code and the tag after it on the same line is still read.
fn code_of(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut quoted = false;
    for index in 0..bytes.len() {
        match bytes[index] {
            b'"' => quoted = !quoted,
            b'/' if !quoted && bytes.get(index + 1) == Some(&b'/') => return &line[..index],
            _ => {}
        }
    }
    line
}

/// The characters a doctype plane name is spelled with.
fn is_tag_character(character: char) -> bool {
    character.is_ascii_lowercase() || character.is_ascii_digit() || matches!(character, '.' | '-')
}

#[test]
fn a_tag_states_its_plane_and_version() {
    assert_eq!(
        control_plane_tags("t.brenn", "  in theme: \"brenn.surface.theme@1\";\n"),
        vec![("theme", 1)]
    );
}

#[test]
fn a_plane_named_in_prose_is_not_a_tag() {
    // The specs are heavily commented, and a comment naming a plane says nothing
    // about a version.
    let source = "// the chrome reads \"brenn.surface.theme\" off the page ring\n\
                  /// and writes brenn.surface.overlay-state\n";
    assert!(control_plane_tags("t.brenn", source).is_empty());
}

#[test]
fn a_tag_beside_a_url_on_one_line_is_still_read() {
    assert_eq!(
        control_plane_tags(
            "t.brenn",
            "  remote = \"https://example.com/a\"; in theme: \"brenn.surface.theme@1\";\n"
        ),
        vec![("theme", 1)]
    );
}

#[test]
#[should_panic(expected = "`brenn.surface.theme` states no `@version`")]
fn a_tag_that_forgot_its_version_is_not_satisfied_by_a_later_one() {
    control_plane_tags(
        "t.brenn",
        "  in theme: \"brenn.surface.theme\";\n  in panel: \"alice.panel@1\";\n",
    );
}

/// The shipped `.brenn` files as text: the roots beside this crate and every
/// module directory under `config/`.
fn config_sources() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut sources = Vec::new();
    let mut dirs = vec![root.clone(), root.join("config")];
    while let Some(dir) = dirs.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", dir.display()));
        for entry in entries {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() && dir.ends_with("config") {
                dirs.push(path);
                continue;
            }
            if path
                .extension()
                .is_some_and(|extension| extension == "brenn")
            {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
                sources.push((path.display().to_string(), text));
            }
        }
    }
    sources
}

/// The DSL prose reference's `Chrome` example is the shipped spec, verbatim.
///
/// `docs/config-dsl.md` transcribes `config/specs/chrome.brenn` whole. Nothing
/// else would notice the day the spec grows a port and the transcription does
/// not, so the equality is held here.
///
/// TODO(dsl-doc-examples-ungated): this is the only block in that document held
/// against anything; the other snippets are prose nothing parses.
#[test]
fn the_dsl_doc_transcribes_the_chrome_spec_verbatim() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let doc_path = root.join("docs/config-dsl.md");
    let spec_path = root.join("config/specs/chrome.brenn");
    let doc = std::fs::read_to_string(&doc_path)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", doc_path.display()));
    let spec = std::fs::read_to_string(&spec_path)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", spec_path.display()));
    let block = fenced_blocks(&doc)
        .into_iter()
        .find(|block| block.contains("component Chrome {"))
        .unwrap_or_else(|| {
            panic!(
                "docs/config-dsl.md carries no fenced block declaring `component Chrome {{`; \
                 the transcription of config/specs/chrome.brenn is gone, not merely stale"
            )
        });
    assert_eq!(
        block.trim_end(),
        spec.trim_end(),
        "the `component Chrome` block in docs/config-dsl.md has drifted from \
         config/specs/chrome.brenn; re-copy the spec into the block"
    );
}

/// The contents of every fenced code block in a markdown document.
///
/// A fence is a line whose first non-space characters are three backticks; an
/// info string after them (e.g. `brenn`) is part of the fence, not of the
/// block, and indentation belongs to neither. The
/// blocks come back with their own lines newline-terminated so a block that
/// transcribes a file compares against the file as read.
fn fenced_blocks(doc: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut open: Option<(usize, String)> = None;
    for line in doc.lines() {
        let indent = line.len() - line.trim_start().len();
        match (line.trim_start().starts_with("```"), open.take()) {
            (true, None) => open = Some((indent, String::new())),
            (true, Some((_, block))) => blocks.push(block),
            (false, Some((indent_of_fence, mut block))) => {
                let stripped =
                    if line.len() >= indent_of_fence && line.is_char_boundary(indent_of_fence) {
                        &line[indent_of_fence..]
                    } else {
                        line.trim_start()
                    };
                block.push_str(stripped);
                block.push('\n');
                open = Some((indent_of_fence, block));
            }
            (false, None) => {}
        }
    }
    assert!(
        open.is_none(),
        "a fenced code block is never closed; the document's fences are unbalanced"
    );
    blocks
}

#[test]
fn a_fenced_block_is_read_without_its_fence_or_indent() {
    let doc = "prose\n\n  ```brenn\n  component A {\n    abi = dom;\n  }\n  ```\n\nmore\n";
    assert_eq!(
        fenced_blocks(doc),
        vec!["component A {\n  abi = dom;\n}\n".to_string()]
    );
}

/// Every root config shipped beside this crate compiles and loads.
///
/// Globbed rather than listed: a new root joins this gate by existing, which is
/// what `docs/config-dsl.md` tells an author it does.
#[test]
fn every_root_config_parses() {
    let roots = root_config_files();
    assert!(
        roots.len() >= 2,
        "found {} root config(s) beside brenn-lib; the dev and e2e roots are both \
         shipped, so this test is reading the wrong tree rather than passing",
        roots.len()
    );
    for filename in roots {
        assert_config_file_messaging_invariant(&filename);
    }
}

/// The names of the `.brenn` files at the repo root, in sorted order.
fn root_config_files() -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let entries = std::fs::read_dir(&root)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", root.display()));
    let mut names: Vec<String> = entries
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "brenn")
        })
        .map(|path| {
            path.file_name()
                .expect("a file name")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}
