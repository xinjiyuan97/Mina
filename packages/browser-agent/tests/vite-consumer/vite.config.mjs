import { defineConfig } from "vite";
const requests = [];
function mockProvider(server) {
  server.middlewares.use(async (req, res, next) => {
    if (req.url === "/test/requests") { res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify(requests)); return; }
    if (req.url !== "/v1/messages") return next();
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const body = JSON.parse(raw);
    requests.push(body);
    const last = body.messages.at(-1);
    const text = last.content.filter((x) => x.type === "text").map((x) => x.text).join(" ");
    const toolResult = last.content.some((x) => x.type === "tool_result");
    const javascript = text.includes("js-tool");
    const tool = (text.includes("write-tool") || javascript) && !toolResult;
    const event = (value) => `data: ${JSON.stringify(value)}\n\n`;
    const events = [{ type: "message_start", message: { id: `mock-${requests.length}`, usage: { input_tokens: 10 } } }];
    if (tool) {
      events.push({ type: "content_block_start", index: 0, content_block: { type: "tool_use", id: `call-${requests.length}`, name: javascript ? "javascript_eval" : "write" } },
        { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify(javascript ? {source: text.includes("endless") ? "export function main(){while(true){}}" : "export function main(input){console.log(\"count\",input.length);return input.reduce((a,b)=>a+b,0)}", input:[3,7,11]} : {path: "proof.txt", content: "SDK tool result"}) } });
    } else {
      events.push({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: toolResult ? "Tool finished" : `Reply: ${text}` } });
    }
    events.push({ type: "message_delta", delta: { stop_reason: tool ? "tool_use" : "end_turn" }, usage: { output_tokens: 5 } }, { type: "message_stop" });
    res.setHeader("Content-Type", "text/event-stream");
    res.end(events.map(event).join(""));
  });
}
export default defineConfig({
  optimizeDeps: { exclude: ["@mina/browser-agent"] },
  base: "/embedded/",
  worker: { format: "es" },
  plugins: [{ name: "local-test-provider", configureServer: mockProvider, configurePreviewServer: mockProvider }],
});
