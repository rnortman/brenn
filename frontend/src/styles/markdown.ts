/**
 * Shared markdown rendering styles.
 *
 * Used by any Shadow DOM component that displays server-rendered markdown HTML.
 * Wrap markdown content in an element with class "md-content" to apply these styles.
 *
 * Consumers include their own `static styles` alongside this:
 *   import { markdownStyles } from "../styles/markdown.js";
 *   static styles = [markdownStyles, css`...component-specific...`];
 *
 * Frontmatter rendering styles (`fm-block`, `fm-list`, etc.) come from
 * the auto-generated `./frontmatter.generated.js` and are re-exported
 * as `frontmatterStyles` for components that render rendered HTML
 * containing a frontmatter block.
 */

import { css } from "lit";

export { frontmatterStyles } from "./frontmatter.generated.js";

export const markdownStyles = css`
  /* === Scoped reset for markdown output elements === */
  /* Global resets (e.g., app.css * reset) don't pierce Shadow DOM. */
  p,
  h1,
  h2,
  h3,
  h4,
  h5,
  h6,
  ul,
  ol,
  li,
  blockquote,
  table,
  thead,
  tbody,
  tr,
  th,
  td,
  hr,
  pre,
  code,
  details,
  summary,
  dl,
  dt,
  dd {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
  }

  /* === Markdown rendered content === */

  .md-content p {
    margin: 0.4rem 0;
  }

  .md-content h1 {
    font-size: 1.4rem;
    font-weight: 700;
    margin: 0.75rem 0 0.25rem;
    color: #e0e0e8;
  }

  .md-content h2 {
    font-size: 1.2rem;
    font-weight: 600;
    margin: 0.6rem 0 0.2rem;
    color: #e0e0e8;
  }

  .md-content h3 {
    font-size: 1.1rem;
    font-weight: 600;
    margin: 0.5rem 0 0.15rem;
    color: #e0e0e8;
  }

  .md-content h4,
  .md-content h5,
  .md-content h6 {
    font-size: 1rem;
    font-weight: 600;
    margin: 0.4rem 0 0.1rem;
    color: #d0d0d8;
  }

  /* Code blocks — layout, font, and background. The backend renders all code
     blocks as plain <pre><code> with no syntax highlighting and no inline
     styles, so a single uniform rule applies. */
  .md-content pre {
    border: 1px solid #2a2a40;
    padding: 0.75rem;
    overflow-x: auto;
    font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
    font-size: 0.9rem;
    line-height: 1.4;
    margin: 0.5rem 0;
    white-space: pre;
    word-wrap: normal;
    background: #141427;
  }

  /* Inline code (not inside pre). */
  .md-content code:not(pre code) {
    background: #222240;
    padding: 0.1rem 0.3rem;
    font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
    font-size: 0.9em;
    border-radius: 2px;
  }

  .md-content ul,
  .md-content ol {
    padding-left: 1.5rem;
    margin: 0.4rem 0;
  }

  .md-content li {
    margin: 0.15rem 0;
  }

  .md-content blockquote {
    border-left: 3px solid #3a3a50;
    padding-left: 0.75rem;
    color: #a0a0b0;
    margin: 0.4rem 0;
  }

  .md-content table {
    border-collapse: collapse;
    margin: 0.5rem 0;
    font-size: 0.9rem;
  }

  .md-content th,
  .md-content td {
    border: 1px solid #2a2a40;
    padding: 0.35rem 0.75rem;
    text-align: left;
  }

  .md-content th {
    background: #1e1e35;
    font-weight: 600;
  }

  .md-content a {
    color: #4a9fd5;
    text-decoration: none;
  }

  .md-content a:hover {
    text-decoration: underline;
  }

  .md-content hr {
    border: none;
    border-top: 1px solid #2a2a40;
    margin: 0.75rem 0;
  }

  .md-content strong {
    color: #e0e0e8;
  }

  .md-content del {
    color: #808098;
  }

  /* Task list checkboxes (pulldown-cmark emits <input type="checkbox">) */
  .md-content input[type="checkbox"] {
    margin-right: 0.3rem;
  }

  /* === Thinking blocks (server-rendered as <details>) === */

  .md-content .thinking-block {
    border-left: 2px solid #3a3a50;
    margin: 0.5rem 0;
    padding-left: 0.5rem;
  }

  .md-content .thinking-block summary {
    color: #707088;
    font-style: italic;
    font-size: 0.9rem;
    cursor: pointer;
    user-select: none;
  }

  .md-content .thinking-block .thinking-content {
    color: #707088;
    font-size: 0.9rem;
  }
`;
