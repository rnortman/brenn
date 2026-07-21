/**
 * <brenn-ask-user-question> — Custom dialog for CC's AskUserQuestion tool.
 *
 * Renders structured multiple-choice questions with vertically stacked buttons
 * and per-question free-form text inputs. Sends answers back via
 * brenn-tool-response events.
 *
 * Data is loaded from an embedded <script type="application/json"> child tag
 * (placed by the backend's AskUserQuestionTool::format_display).
 *
 * Single-select single-question dialogs auto-submit on click or number key.
 * Freeform textarea respects the enterSends setting from the embedded config.
 *
 * Shadow DOM for markdown style encapsulation.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";
import { unsafeHTML } from "lit/directives/unsafe-html.js";
import { markdownStyles } from "../styles/markdown.js";
import { registerMount, unregisterMount, canInterceptKeyboard, eventOriginatedInside } from "../keyboard-guard.js";

/** Shape of a single question from CC's AskUserQuestion tool input. */
interface AuqQuestion {
  question: string;
  header: string;
  options: AuqOption[];
  multiSelect: boolean;
}

interface AuqOption {
  label: string;
  description: string;
}

/** Server-rendered HTML for a question (from rendered). */
interface RenderedQuestion {
  question_html: string;
  options: { label_html: string; description_html: string }[];
}

/** JSON payload embedded in the <script> tag by the backend. */
interface AuqPayload {
  questions: AuqQuestion[];
  rendered: { questions: RenderedQuestion[] };
  enter_sends: boolean;
}

/** Max textarea height in px before scrollbar appears. */
const MAX_TEXT_HEIGHT = 120;

@customElement("brenn-ask-user-question")
export class BrennAskUserQuestion extends LitElement {
  static styles = [
    markdownStyles,
    css`
      :host {
        display: flex;
        flex-direction: column;
        flex-shrink: 1;
        min-height: 0;
      }

      .auq-card {
        background: #1a1a30;
        border: 1px solid #2a2a4a;
        border-radius: 2px;
        padding: 0.75rem;
        overflow-y: auto;
        min-height: 0;
        scrollbar-color: #2a2a40 transparent;
        scrollbar-width: thin;
      }

      .auq-question {
        margin-bottom: 1rem;
      }

      .auq-question:last-of-type {
        margin-bottom: 0.5rem;
      }

      .auq-header {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #707088;
        margin-bottom: 0.25rem;
      }

      .auq-question-text {
        font-size: 1rem;
        color: #d0d0d8;
        margin-bottom: 0.75rem;
        line-height: 1.5;
      }

      /* Strip wrapping <p> margins for inline question text */
      .auq-question-text > p:only-child {
        margin: 0;
      }

      .auq-options {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
        margin-bottom: 0.5rem;
      }

      .auq-option-btn {
        display: block;
        width: 100%;
        min-height: 48px;
        padding: 0.625rem 0.75rem;
        background: #222240;
        border: 2px solid #2a2a4a;
        border-radius: 2px;
        color: #d0d0d8;
        cursor: pointer;
        text-align: left;
        font-size: 1rem;
        font-family: inherit;
        touch-action: manipulation;
        transition:
          border-color 0.15s,
          background 0.15s;
      }

      .auq-option-btn:hover {
        border-color: #3a3a60;
        background: #282850;
      }

      .auq-option-btn:focus-visible {
        outline: 2px solid #4a9fd5;
        outline-offset: 2px;
      }

      .auq-option-btn[aria-pressed="true"] {
        border-color: #4a9fd5;
        background: #1e2e50;
      }

      .auq-option-label {
        font-weight: 500;
      }

      .auq-option-number {
        color: #707088;
        margin-right: 0.25rem;
      }

      .auq-option-desc {
        font-size: 0.85rem;
        color: #888;
        margin-top: 0.15rem;
        line-height: 1.4;
      }

      /* Strip wrapping <p> margins from inline markdown in labels/descriptions */
      .auq-option-label > p:only-child,
      .auq-option-desc > p:only-child {
        margin: 0;
      }

      .auq-text-input {
        width: 100%;
        min-height: 40px;
        padding: 0.5rem 0.75rem;
        background: #1f2b47;
        color: #d0d0d8;
        border: 1px solid #2a2a4a;
        border-radius: 2px;
        font-size: 0.9rem;
        font-family: inherit;
        outline: none;
        box-sizing: border-box;
        resize: none;
        height: auto;
        max-height: 120px;
        overflow-y: hidden;
        line-height: 1.4;
      }

      .auq-text-input::placeholder {
        color: #606078;
      }

      .auq-text-input:focus {
        border-color: #4a9fd5;
      }

      .auq-actions {
        display: flex;
        gap: 0.5rem;
        justify-content: flex-end;
        margin-top: 0.75rem;
      }

      .auq-submit,
      .auq-cancel {
        min-height: 40px;
        padding: 0.5rem 1.25rem;
        border: none;
        border-radius: 2px;
        font-size: 0.9rem;
        font-family: inherit;
        cursor: pointer;
        font-weight: 500;
        touch-action: manipulation;
      }

      .auq-submit {
        background: #2d6b3f;
        color: white;
      }

      .auq-submit:hover:not(:disabled) {
        opacity: 0.9;
      }

      .auq-submit:disabled {
        opacity: 0.4;
        cursor: not-allowed;
      }

      .auq-cancel {
        background: #3a3a50;
        color: #d0d0d8;
      }

      .auq-cancel:hover {
        opacity: 0.9;
      }
    `,
  ];

  /** Parsed payload from embedded script tag. */
  private payload: AuqPayload | null = null;

  /** Per-question selected option indices (single-select: one index; multi-select: set). */
  @state() private selections: Map<number, Set<number>> = new Map();
  /** Per-question free-form text. */
  @state() private textInputs: Map<number, string> = new Map();

  private boundKeyHandler = this.handleKeydown.bind(this);

  connectedCallback(): void {
    super.connectedCallback();

    // Read payload from embedded <script type="application/json"> child.
    // this.children accesses light DOM children regardless of Shadow DOM.
    const scriptEl = Array.from(this.children).find(
      (el): el is HTMLScriptElement =>
        el instanceof HTMLScriptElement && el.type === "application/json",
    );
    if (scriptEl) {
      try {
        this.payload = JSON.parse(scriptEl.textContent ?? "{}") as AuqPayload;
      } catch {
        this.payload = null;
      }
    }

    // Reset state for fresh display.
    this.selections = new Map();
    this.textInputs = new Map();

    registerMount(this);
    document.addEventListener("keydown", this.boundKeyHandler);

    // No focus steal — the chat textarea keeps focus.
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    unregisterMount(this);
    document.removeEventListener("keydown", this.boundKeyHandler);
  }

  private get questions(): AuqQuestion[] {
    return this.payload?.questions ?? [];
  }

  private get rendered(): RenderedQuestion[] {
    return this.payload?.rendered?.questions ?? [];
  }

  private get enterSends(): boolean {
    return this.payload?.enter_sends ?? true;
  }

  /** Whether this dialog can auto-submit on single click/number key. */
  private get isQuickSubmit(): boolean {
    const qs = this.questions;
    return qs.length === 1 && !qs[0].multiSelect;
  }

  private get allAnswered(): boolean {
    for (let qi = 0; qi < this.questions.length; qi++) {
      const text = this.textInputs.get(qi) ?? "";
      const selected = this.selections.get(qi);
      if (text.trim() === "" && (!selected || selected.size === 0)) {
        return false;
      }
    }
    return this.questions.length > 0;
  }

  render() {
    if (!this.payload) return nothing;

    const questions = this.questions;
    const rendered = this.rendered;

    return html`
      <div class="auq-card">
        ${questions.map((q, qi) => this.renderQuestion(q, qi, rendered[qi]))}
        <div class="auq-actions">
          <button
            class="auq-cancel"
            @click=${this.handleCancel}
            title="Cancel (Esc)"
          >
            Cancel
          </button>
          <button
            class="auq-submit"
            ?disabled=${!this.allAnswered}
            @click=${this.handleSubmit}
            title="Submit (Enter)"
          >
            Submit
          </button>
        </div>
      </div>
    `;
  }

  private renderQuestion(
    q: AuqQuestion,
    qi: number,
    rendered?: RenderedQuestion,
  ) {
    const selected = this.selections.get(qi) ?? new Set<number>();
    const textValue = this.textInputs.get(qi) ?? "";

    return html`
      <div class="auq-question">
        <div class="auq-header">${q.header}</div>
        <div class="auq-question-text md-content">
          ${rendered?.question_html
            ? unsafeHTML(rendered.question_html)
            : q.question}
        </div>
        <div class="auq-options" role="group" aria-label=${q.header}>
          ${q.options.map((opt, oi) => {
            const isSelected = selected.has(oi);
            const renderedOpt = rendered?.options?.[oi];
            return html`
              <button
                class="auq-option-btn"
                role="option"
                aria-pressed=${isSelected ? "true" : "false"}
                data-qi=${qi}
                data-oi=${oi}
                @click=${() => this.handleOptionClick(qi, oi, q.multiSelect)}
              >
                <div class="auq-option-label md-content">
                  <span class="auq-option-number">${oi + 1}.</span>
                  ${renderedOpt?.label_html
                    ? unsafeHTML(renderedOpt.label_html)
                    : opt.label}
                </div>
                <div class="auq-option-desc md-content">
                  ${renderedOpt?.description_html
                    ? unsafeHTML(renderedOpt.description_html)
                    : opt.description}
                </div>
              </button>
            `;
          })}
        </div>
        <textarea
          class="auq-text-input"
          rows="1"
          placeholder="Or type your answer\u2026"
          .value=${textValue}
          data-qi=${qi}
          @input=${(e: Event) => this.handleTextInput(qi, e)}
          @keydown=${(e: KeyboardEvent) => this.handleTextKeydown(qi, e)}
        ></textarea>
      </div>
    `;
  }

  private handleOptionClick(
    qi: number,
    oi: number,
    multiSelect: boolean,
  ): void {
    const newSelections = new Map(this.selections);
    const current = new Set(newSelections.get(qi) ?? []);

    if (multiSelect) {
      if (current.has(oi)) {
        current.delete(oi);
      } else {
        current.add(oi);
      }
    } else {
      current.clear();
      current.add(oi);
    }

    newSelections.set(qi, current);
    this.selections = newSelections;

    // Mutual exclusivity: clear text input when clicking a button.
    const newTexts = new Map(this.textInputs);
    newTexts.set(qi, "");
    this.textInputs = newTexts;

    // Also clear the actual textarea element.
    const textarea = this.shadowRoot?.querySelector<HTMLTextAreaElement>(
      `textarea[data-qi="${qi}"]`,
    );
    if (textarea) {
      textarea.value = "";
      this.autoResizeText(textarea);
    }

    // Auto-submit for single-select single-question.
    if (this.isQuickSubmit) {
      this.handleSubmit();
    }
  }

  private handleTextInput(qi: number, e: Event): void {
    const value = (e.target as HTMLTextAreaElement).value;
    const newTexts = new Map(this.textInputs);
    newTexts.set(qi, value);
    this.textInputs = newTexts;

    // Mutual exclusivity: deselect buttons when typing.
    if (value.trim() !== "") {
      const newSelections = new Map(this.selections);
      newSelections.set(qi, new Set());
      this.selections = newSelections;
    }

    // Auto-resize the textarea.
    this.autoResizeText(e.target as HTMLTextAreaElement);
  }

  private handleTextKeydown(_qi: number, e: KeyboardEvent): void {
    if (e.key !== "Enter") return;

    // Stop propagation so the document-level handler doesn't double-process.
    // Shadow DOM retargets e.target to the host element, which bypasses
    // the instanceof HTMLTextAreaElement check in handleKeydown.
    e.stopPropagation();

    const isModified = e.ctrlKey || e.metaKey;
    if (isModified) {
      // Ctrl/Cmd+Enter always submits.
      e.preventDefault();
      if (this.allAnswered) this.handleSubmit();
    } else if (e.shiftKey) {
      // Shift+Enter always newline (browser default).
    } else if (this.enterSends) {
      // Plain Enter submits when enterSends is on.
      e.preventDefault();
      if (this.allAnswered) this.handleSubmit();
    }
    // Otherwise plain Enter adds newline (browser default).
  }

  private autoResizeText(el: HTMLTextAreaElement): void {
    el.style.height = "auto";
    el.style.overflowY = "hidden";
    if (el.scrollHeight > MAX_TEXT_HEIGHT) {
      el.style.height = `${MAX_TEXT_HEIGHT}px`;
      el.style.overflowY = "auto";
    } else {
      el.style.height = `${el.scrollHeight}px`;
    }
  }

  private handleSubmit(): void {
    if (!this.allAnswered) return;

    const questions = this.questions;
    const answers: Record<string, string> = {};

    for (let qi = 0; qi < questions.length; qi++) {
      const q = questions[qi];
      const text = (this.textInputs.get(qi) ?? "").trim();

      if (text !== "") {
        answers[q.question] = text;
      } else {
        const selected = this.selections.get(qi) ?? new Set<number>();
        const labels = Array.from(selected)
          .sort()
          .map((oi) => q.options[oi].label);
        answers[q.question] = labels.join(", ");
      }
    }

    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: { questions: questions, answers: answers },
      }),
    );
  }

  private handleCancel(): void {
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: { deny: true },
      }),
    );
  }

  private handleKeydown(e: KeyboardEvent): void {
    // Don't intercept keyboard when the user is typing in chat or during grace period.
    if (!canInterceptKeyboard(this)) return;

    if (e.key === "Escape") {
      e.preventDefault();
      this.handleCancel();
      return;
    }

    if (e.key === "Enter") {
      // Don't intercept Enter in text inputs — handled by handleTextKeydown.
      const target = e.target as HTMLElement;
      if (
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLInputElement
      ) {
        return;
      }
      // Only accept if the keydown originated inside this dialog's subtree.
      if (!eventOriginatedInside(e, this)) return;

      if (this.allAnswered) {
        e.preventDefault();
        this.handleSubmit();
      }
      return;
    }

    // Number keys (1-9) select option — skip when focus is in text input.
    const num = parseInt(e.key, 10);
    if (num >= 1 && num <= 9) {
      const target = e.target as HTMLElement;
      if (
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLInputElement
      ) {
        return;
      }
      // Only accept if the keydown originated inside this dialog's subtree.
      if (!eventOriginatedInside(e, this)) return;

      // Apply to the first question. Multi-question dialogs are rare in practice;
      // number keys only target question 0. Arrow keys + Enter still work for others.
      const qi = 0;
      const oi = num - 1;
      const q = this.questions[qi];
      if (q && oi < q.options.length) {
        e.preventDefault();
        this.handleOptionClick(qi, oi, q.multiSelect);
      }
      return;
    }

    // Arrow key navigation between option buttons.
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      const focused = this.shadowRoot?.activeElement;
      if (focused instanceof HTMLButtonElement && focused.dataset.qi) {
        const buttons = Array.from(
          this.shadowRoot?.querySelectorAll<HTMLButtonElement>(
            `.auq-option-btn[data-qi="${focused.dataset.qi}"]`,
          ) ?? [],
        );
        const idx = buttons.indexOf(focused);
        if (idx >= 0) {
          const next =
            e.key === "ArrowDown"
              ? buttons[idx + 1] ?? buttons[0]
              : buttons[idx - 1] ?? buttons[buttons.length - 1];
          e.preventDefault();
          next.focus();
        }
      }
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-ask-user-question": BrennAskUserQuestion;
  }
}
