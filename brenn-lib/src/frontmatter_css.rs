//! Source of truth for the frontmatter block's CSS rules.
//!
//! Lives in `brenn-lib` (rather than alongside the renderer in
//! `brenn/src/frontmatter.rs`) so that `brenn-cli emit-frontmatter-css`
//! can pull it for build-time generation of the matching frontend
//! constant. The renderer module re-exports it as
//! `brenn::frontmatter::FRONTMATTER_CSS`.
//!
//! See `docs/designs/frontmatter-rendering.md`.

/// Static CSS for the frontmatter block.
///
/// Three consumers, one source:
/// - `brenn/src/routes/file.rs`: interpolated into the `<style>` block of
///   the `/app/<slug>/file/...` route.
/// - `frontend/src/styles/frontmatter.generated.ts`: written at build
///   time by `brenn-cli emit-frontmatter-css`, imported by Shadow-DOM
///   components.
/// - Anything else that wants the rules: import from this module.
pub const FRONTMATTER_CSS: &str = r#".fm-block {
  font-size: 0.9em;
  color: #a0a0b0;
  border-bottom: 1px solid #2a2a40;
  padding-bottom: 0.5rem;
  margin-bottom: 0.75rem;
}
.fm-list {
  display: grid;
  grid-template-columns: max-content 1fr;
  column-gap: 0.75rem;
  row-gap: 0.15rem;
  margin: 0;
  padding: 0;
}
.fm-row {
  display: contents;
}
.fm-list dt {
  font-weight: 600;
  color: #808098;
  margin: 0;
  padding: 0;
}
.fm-list dd {
  margin: 0;
  padding: 0;
  min-width: 0;
  word-break: normal;
}
.fm-md p {
  margin: 0;
}
.fm-sublist {
  list-style: none;
  padding-left: 0;
  margin: 0;
}
.fm-sublist li {
  margin: 0;
}
.fm-truncated {
  font-style: italic;
  color: #808098;
}
.fm-null {
  color: #6a6a8a;
  font-style: italic;
}
.fm-raw {
  font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
  font-size: 0.95em;
  color: #b0b0c0;
}
.fm-error {
  color: #c08080;
  border: 1px solid #c08080;
  padding: 0.5rem;
  margin-bottom: 0.75rem;
  border-radius: 3px;
}
.fm-error pre {
  margin: 0;
  white-space: pre-wrap;
}
"#;
