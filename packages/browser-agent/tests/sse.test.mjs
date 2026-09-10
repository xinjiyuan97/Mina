import assert from "node:assert/strict";
import test from "node:test";

import { readServerSentEvents } from "../dist/internal/sse.js";

test("parses chunked, multiline server-sent events", async () => {
  const encoder = new TextEncoder();
  const chunks = [
    "event: response.output_text.delta\r\ndata: {\"delta\":",
    "\"hello\"}\r\n\r\n: keepalive\n",
    "data: first\ndata: second\n\n",
  ];
  const body = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });

  const events = [];
  for await (const event of readServerSentEvents(body)) events.push(event);

  assert.deepEqual(events, [
    {
      event: "response.output_text.delta",
      data: '{"delta":"hello"}',
    },
    { event: "message", data: "first\nsecond" },
  ]);
});

test("flushes a final event without a blank-line terminator", async () => {
  const body = new Blob(["data: [DONE]"]).stream();
  const events = [];
  for await (const event of readServerSentEvents(body)) events.push(event);
  assert.deepEqual(events, [{ event: "message", data: "[DONE]" }]);
});
