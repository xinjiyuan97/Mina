import assert from "node:assert/strict";
import test from "node:test";

import {
  responsesContinuationInput,
  responsesInput,
  responsesStatelessContinuationInput,
} from "../dist/internal/responses-input.js";
import { ResponsesProviderToolTracker } from "../dist/internal/responses-provider-tools.js";
import {
  MessagesReasoningTracker,
  ResponsesReasoningTracker,
} from "../dist/internal/reasoning-events.js";
import {
  attachmentIdFromUrl,
  inferMediaType,
  stableAttachmentUrl,
} from "../dist/internal/attachment-store.js";
import {
  messagesAttachmentBlock,
  responsesAttachmentPart,
} from "../dist/internal/multimodal-input.js";

test("uses Responses content types that match each message role", () => {
  assert.deepEqual(
    responsesInput([
      { role: "system", content: "Be concise." },
      { role: "user", content: "List my files." },
      {
        role: "assistant",
        content: "I will inspect them.",
        tool_calls: [{ id: "call_1", name: "list_files", arguments: "{}" }],
      },
      { role: "tool", content: '{"files":[]}', tool_call_id: "call_1" },
    ]),
    [
      {
        role: "system",
        content: [{ type: "input_text", text: "Be concise." }],
      },
      {
        role: "user",
        content: [{ type: "input_text", text: "List my files." }],
      },
      {
        role: "assistant",
        content: [{ type: "output_text", text: "I will inspect them." }],
      },
      {
        type: "function_call",
        call_id: "call_1",
        name: "list_files",
        arguments: "{}",
      },
      {
        type: "function_call_output",
        call_id: "call_1",
        output: '{"files":[]}',
      },
    ],
  );
});

test("sends only new tool outputs when continuing a Responses request", () => {
  assert.deepEqual(
    responsesContinuationInput([
      { role: "system", content: "Be concise." },
      { role: "user", content: "List my files." },
      {
        role: "assistant",
        content: "I will inspect them.",
        tool_calls: [{ id: "call_1", name: "list_files", arguments: "{}" }],
      },
      { role: "tool", content: '{"files":[]}', tool_call_id: "call_1" },
    ]),
    [
      {
        type: "function_call_output",
        call_id: "call_1",
        output: '{"files":[]}',
      },
    ],
  );
});

test("replays exact Responses output items before function outputs", () => {
  const reasoning = {
    type: "reasoning",
    id: "rs_1",
    encrypted_content: "encrypted-reasoning",
    summary: [],
  };
  const call = {
    type: "function_call",
    id: "fc_1",
    call_id: "call_1",
    name: "list_directory",
    arguments: "{}",
  };
  assert.deepEqual(
    responsesStatelessContinuationInput(
      [
        { role: "user", content: [{ type: "input_text", text: "List files" }] },
        reasoning,
        call,
      ],
      [
        {
          role: "assistant",
          content: "",
          tool_calls: [{ id: "call_1", name: "list_directory", arguments: "{}" }],
        },
        { role: "tool", content: '{"entries":[]}', tool_call_id: "call_1" },
      ],
    ),
    [
      { role: "user", content: [{ type: "input_text", text: "List files" }] },
      reasoning,
      call,
      {
        type: "function_call_output",
        call_id: "call_1",
        output: '{"entries":[]}',
      },
    ],
  );
});

test("maps Responses web search lifecycle without creating a local function call", () => {
  const tracker = new ResponsesProviderToolTracker();

  assert.deepEqual(
    tracker.push("response.web_search_call.in_progress", {
      item_id: "ws_1",
    }),
    [
      {
        type: "provider_tool_call_started",
        call_id: "ws_1",
        name: "web_search",
        arguments: { status: "in_progress" },
      },
    ],
  );
  assert.deepEqual(
    tracker.push("response.web_search_call.searching", {
      item_id: "ws_1",
    }),
    [],
  );
  assert.deepEqual(
    tracker.push("response.output_item.done", {
      item: {
        type: "web_search_call",
        id: "ws_1",
        status: "completed",
        action: { type: "search", query: "Mina WASM agent" },
      },
    }),
    [
      {
        type: "provider_tool_call_completed",
        call_id: "ws_1",
        output: JSON.stringify({
          status: "completed",
          action: { type: "search", query: "Mina WASM agent" },
        }),
      },
    ],
  );
});

test("finishes a Responses provider tool from the terminal response as a fallback", () => {
  const tracker = new ResponsesProviderToolTracker();
  tracker.push("response.output_item.added", {
    item: { type: "web_search_call", id: "ws_2", status: "in_progress" },
  });

  assert.deepEqual(
    tracker.push("response.completed", {
      response: {
        output: [
          {
            type: "web_search_call",
            id: "ws_2",
            status: "completed",
            action: { type: "open_page", url: "https://example.com" },
          },
        ],
      },
    }),
    [
      {
        type: "provider_tool_call_completed",
        call_id: "ws_2",
        output: JSON.stringify({
          status: "completed",
          action: { type: "open_page", url: "https://example.com" },
        }),
      },
    ],
  );
});

test("maps a Responses reasoning item even when it has no text", () => {
  const tracker = new ResponsesReasoningTracker();

  assert.deepEqual(
    tracker.push("response.output_item.added", {
      output_index: 0,
      item: { type: "reasoning", id: "rs_1", summary: [] },
    }),
    [{ type: "reasoning_started", redacted: true }],
  );
  assert.deepEqual(
    tracker.push("response.output_item.done", {
      output_index: 0,
      item: { type: "reasoning", id: "rs_1", summary: [] },
    }),
    [{ type: "reasoning_completed", redacted: true }],
  );
});

test("maps Messages thinking and redacted thinking block lifecycles", () => {
  const tracker = new MessagesReasoningTracker();

  assert.deepEqual(
    tracker.push("content_block_start", {
      index: 0,
      content_block: { type: "thinking", thinking: "" },
    }),
    [{ type: "reasoning_started", redacted: false }],
  );
  assert.deepEqual(tracker.push("content_block_stop", { index: 0 }), [
    { type: "reasoning_completed", redacted: false },
  ]);
  assert.deepEqual(
    tracker.push("content_block_start", {
      index: 1,
      content_block: { type: "redacted_thinking", data: "opaque" },
    }),
    [{ type: "reasoning_started", redacted: true }],
  );
  assert.deepEqual(tracker.push("content_block_stop", { index: 1 }), [
    { type: "reasoning_completed", redacted: true },
  ]);
});

test("keeps an OPFS blob id recoverable from stable and display URLs", () => {
  const blobId = "8ba57b28-646b-4bbd-bd23-63d6dc1bce7a";

  assert.equal(attachmentIdFromUrl(stableAttachmentUrl(blobId)), blobId);
  assert.equal(
    attachmentIdFromUrl(`blob:http://127.0.0.1:3002/object#mina-attachment=${blobId}`),
    blobId,
  );
  assert.equal(inferMediaType("notes.md", ""), "text/markdown");
});

test("maps image and document attachments to Responses input parts", () => {
  assert.deepEqual(
    responsesAttachmentPart(encodedAttachment("image/png", "screen.png")),
    {
      type: "input_image",
      image_url: "data:image/png;base64,Ynl0ZXM=",
      detail: "auto",
    },
  );
  assert.deepEqual(
    responsesAttachmentPart(encodedAttachment("application/pdf", "report.pdf")),
    {
      type: "input_file",
      filename: "report.pdf",
      file_data: "data:application/pdf;base64,Ynl0ZXM=",
    },
  );
});

test("maps image and PDF attachments to Messages content blocks", () => {
  assert.deepEqual(
    messagesAttachmentBlock(encodedAttachment("image/webp", "photo.webp")),
    {
      type: "image",
      source: { type: "base64", media_type: "image/webp", data: "Ynl0ZXM=" },
    },
  );
  assert.deepEqual(
    messagesAttachmentBlock(encodedAttachment("application/pdf", "report.pdf")),
    {
      type: "document",
      source: { type: "base64", media_type: "application/pdf", data: "Ynl0ZXM=" },
      title: "report.pdf",
    },
  );
});

function encodedAttachment(mediaType, name) {
  return {
    metadata: {
      schema_version: 1,
      blob_id: "8ba57b28-646b-4bbd-bd23-63d6dc1bce7a",
      media_type: mediaType,
      name,
      size_bytes: 5,
      created_at_ms: 1,
    },
    base64: "Ynl0ZXM=",
    dataUrl: `data:${mediaType};base64,Ynl0ZXM=`,
  };
}
