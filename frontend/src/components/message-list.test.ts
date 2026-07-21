// @vitest-environment happy-dom
import { describe, it, expect, afterEach } from "vitest";
import "./message-list.js";
import type {
  BrennMessageList,
  MessageBatchItem,
  SystemMessageInput,
  UserMessageInput,
} from "./message-list.js";
import type { HistoryPageMessage } from "../generated/HistoryPageMessage.js";

afterEach(() => {
  document.body.replaceChildren();
});

const SYSTEM_RENDERED_HTML =
  '<details class="brenn-system brenn-system-compaction-reminder"><summary>Compaction reminder (context 75%)</summary><div class="brenn-system-body"><p>example body</p></div></details>';

function systemInput(
  overrides: Partial<SystemMessageInput> = {},
): SystemMessageInput {
  return {
    renderedHtml: SYSTEM_RENDERED_HTML,
    category: "CompactionReminder",
    ...overrides,
  };
}

function userInput(overrides: Partial<UserMessageInput> = {}): UserMessageInput {
  return {
    text: "hi from alice",
    username: "alice",
    timestamp: new Date().toISOString(),
    isSelf: true,
    attachments: [],
    selectedTasks: [],
    ...overrides,
  };
}

describe("appendSystemMessage / _buildSystemCard", () => {
  it("renders a system card with the correct classes and HTML", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    el.appendSystemMessage(systemInput());

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const card = container!.querySelector(".msg-system") as HTMLElement | null;
    expect(card).toBeTruthy();
    // Carries the kebab-case category class.
    expect(card!.classList.contains("msg-system-compaction-reminder")).toBe(
      true,
    );
    // innerHTML contains the supplied HTML (the <details> card).
    expect(card!.innerHTML).toContain("brenn-system-compaction-reminder");
    expect(card!.innerHTML).toContain("Compaction reminder (context 75%)");
    // Must NOT have the blue-border-applying .msg-user class.
    expect(card!.classList.contains("msg-user")).toBe(false);
    // The flat-bubble attribution row must NOT be rendered for system cards.
    expect(card!.querySelector(".msg-attribution")).toBeFalsy();
    // The flat-bubble text fallback must NOT be rendered as a separate node.
    expect(card!.querySelector(".msg-text")).toBeFalsy();
  });

  it("applies msg-system-ui-error class for the UiError category", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const uiErrorHtml =
      '<details class="brenn-system brenn-system-ui-error" open><summary>UI tool error: graf_todo_done</summary><div class="brenn-system-body"><p>example error</p></div></details>';
    el.appendSystemMessage(systemInput({ category: "UiError", renderedHtml: uiErrorHtml }));

    const container = el.shadowRoot?.querySelector(".message-scroll");
    const card = container!.querySelector(".msg-system") as HTMLElement | null;
    expect(card).toBeTruthy();
    // Must carry the ui-error CSS class on the outer wrapper.
    expect(card!.classList.contains("msg-system-ui-error")).toBe(true);
    // Must NOT carry any other category class.
    expect(card!.classList.contains("msg-system-compaction-reminder")).toBe(false);
    // Must NOT carry the blue-border .msg-user class.
    expect(card!.classList.contains("msg-user")).toBe(false);
    // innerHTML carries the rendered card (with open attribute for R7).
    expect(card!.innerHTML).toContain("brenn-system-ui-error");
  });
});

describe("appendUserMessage / _buildUserMessageEl", () => {
  it("renders a flat bubble for chat-input origin", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    el.appendUserMessage(userInput());

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();
    // System class must NOT be applied.
    expect(bubble!.classList.contains("msg-system")).toBe(false);
    // Attribution row IS present on the flat-bubble path.
    expect(bubble!.querySelector(".msg-attribution")).toBeTruthy();
    // Text node IS present.
    const textEl = bubble!.querySelector(".msg-text") as HTMLElement | null;
    expect(textEl).toBeTruthy();
    expect(textEl!.textContent).toBe("hi from alice");
  });

  it("renders attachment thumbnail via _renderAttachments on live path", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    el.appSlug = "myapp";
    document.body.appendChild(el);
    await el.updateComplete;

    el.appendUserMessage(
      userInput({
        attachments: [
          {
            upload_id: "u3",
            filename: "live.png",
            media_type: "image/png",
            size: 512,
          },
        ],
      }),
    );

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();

    const attachDiv = bubble!.querySelector(
      ".msg-attachments",
    ) as HTMLElement | null;
    expect(attachDiv).toBeTruthy();

    const img = attachDiv!.querySelector(
      "img.msg-attachment-thumb",
    ) as HTMLImageElement | null;
    expect(img).toBeTruthy();
    expect(img!.src).toContain("/app/myapp/attachment/u3/live.png");
  });
});

describe("bulkAppend", () => {
  it("renders a system card from a system-kind MessageBatchItem (review F11)", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const items: MessageBatchItem[] = [
      { kind: "system", ...systemInput() },
    ];
    el.bulkAppend(items);
    await el.updateComplete;

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const card = container!.querySelector(".msg-system") as HTMLElement | null;
    expect(card).toBeTruthy();
    expect(card!.classList.contains("msg-system-compaction-reminder")).toBe(
      true,
    );
    expect(card!.innerHTML).toContain("Compaction reminder (context 75%)");
    // Must NOT have .msg-user.
    expect(card!.classList.contains("msg-user")).toBe(false);
  });

  it("renders a flat bubble from a user-kind MessageBatchItem", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const items: MessageBatchItem[] = [
      { kind: "user", ...userInput() },
    ];
    el.bulkAppend(items);
    await el.updateComplete;

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();
    expect(bubble!.classList.contains("msg-system")).toBe(false);
  });
});

describe("prependMessages", () => {
  it("renders system card from a system-origin HistoryPageMessage", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 1,
        role: "user",
        rendered_html: SYSTEM_RENDERED_HTML,
        timestamp: new Date().toISOString(),
        category: "CompactionReminder",
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const card = container!.querySelector(".msg-system") as HTMLElement | null;
    expect(card).toBeTruthy();
    expect(card!.classList.contains("msg-system-compaction-reminder")).toBe(true);
    expect(card!.innerHTML).toContain("brenn-system-compaction-reminder");
    expect(card!.innerHTML).toContain("Compaction reminder (context 75%)");
    // Must NOT have .msg-user or .msg-assistant.
    expect(card!.classList.contains("msg-user")).toBe(false);
    expect(card!.classList.contains("msg-assistant")).toBe(false);
  });

  it("renders flat bubble for chat user HistoryPageMessage", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 2,
        role: "user",
        rendered_html: '<div class="msg-text">hello</div>',
        timestamp: new Date().toISOString(),
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();
    expect(bubble!.classList.contains("msg-system")).toBe(false);
    expect(bubble!.classList.contains("msg-assistant")).toBe(false);
    expect(bubble!.innerHTML).toContain("msg-text");
    expect(bubble!.innerHTML).toContain("hello");
  });

  it("renders assistant message for assistant HistoryPageMessage", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 3,
        role: "assistant",
        rendered_html: "<p>hello from assistant</p>",
        timestamp: new Date().toISOString(),
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const el2 = container!.querySelector(".msg-assistant") as HTMLElement | null;
    expect(el2).toBeTruthy();
    expect(el2!.classList.contains("md-content")).toBe(true);
    expect(el2!.classList.contains("msg-system")).toBe(false);
    expect(el2!.classList.contains("msg-user")).toBe(false);
  });

  it("renders attachment thumbnail for image attachment on user message", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    el.appSlug = "myapp";
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 4,
        role: "user",
        rendered_html: '<div class="msg-text">see photo</div>',
        timestamp: new Date().toISOString(),
        attachments: [
          {
            upload_id: "u1",
            // Filename with a space to verify encodeURIComponent is applied.
            filename: "my photo.png",
            media_type: "image/png",
            size: 1024,
          },
        ],
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();

    const attachDiv = bubble!.querySelector(".msg-attachments") as HTMLElement | null;
    expect(attachDiv).toBeTruthy();

    const img = attachDiv!.querySelector("img.msg-attachment-thumb") as HTMLImageElement | null;
    expect(img).toBeTruthy();
    // Space must be percent-encoded in URL, not raw.
    expect(img!.src).toContain("/app/myapp/attachment/u1/my%20photo.png");
    expect(img!.alt).toBe("my photo.png");

    const link = attachDiv!.querySelector("a.msg-attachment-thumb-link") as HTMLAnchorElement | null;
    expect(link).toBeTruthy();
    expect(link!.href).toContain("/app/myapp/attachment/u1/my%20photo.png");
    expect(link!.target).toBe("_blank");
  });

  it("renders file chip for non-image attachment on user message", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    el.appSlug = "myapp";
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 5,
        role: "user",
        rendered_html: '<div class="msg-text">see doc</div>',
        timestamp: new Date().toISOString(),
        attachments: [
          {
            upload_id: "u2",
            filename: "report.pdf",
            media_type: "application/pdf",
            size: 5120,
          },
        ],
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();

    const attachDiv = bubble!.querySelector(".msg-attachments") as HTMLElement | null;
    expect(attachDiv).toBeTruthy();

    const chip = attachDiv!.querySelector("a.msg-attachment-chip") as HTMLAnchorElement | null;
    expect(chip).toBeTruthy();
    expect(chip!.href).toContain("/app/myapp/attachment/u2/report.pdf");
    expect(chip!.textContent).toContain("report.pdf");
    expect(chip!.target).toBe("_blank");
  });

  it("renders no attachment div when attachments absent", async () => {
    const el = document.createElement("brenn-message-list") as BrennMessageList;
    el.appSlug = "myapp";
    document.body.appendChild(el);
    await el.updateComplete;

    const msgs: HistoryPageMessage[] = [
      {
        seq: 6,
        role: "user",
        rendered_html: '<div class="msg-text">plain text</div>',
        timestamp: new Date().toISOString(),
        // No attachments field.
      },
    ];
    el.prependMessages(msgs);

    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    const bubble = container!.querySelector(".msg-user") as HTMLElement | null;
    expect(bubble).toBeTruthy();

    const attachDiv = bubble!.querySelector(".msg-attachments");
    expect(attachDiv).toBeNull();
  });
});
