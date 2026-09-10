import assert from "node:assert/strict";
import test from "node:test";
import { loadQuickJS, evaluateJavaScript } from "../dist/internal/quickjs-engine.js";
import { validateJavaScriptRequest } from "../dist/internal/javascript-contract.js";
import { executeJavaScript } from "../dist/internal/javascript-tool.js";
const engine = await loadQuickJS();
const evaluate = (source, input) => evaluateJavaScript(engine, {source, input});

test("actual QuickJS evaluates JSON transforms and captures bounded logs", () => {
  const result = evaluate('export function main(input) { console.log("rows", input.length); return { sum: input.reduce((a,b) => a+b, 0) }; }', [3,7,11]);
  assert.deepEqual(result.value, {sum:21});
  assert.deepEqual(result.logs, [{level:"log",text:"rows 3"}]);
  assert.ok(result.usage.output_bytes > 0);
});
test("async functions, top-level await and named exports work", () => {
  assert.equal(evaluate('await Promise.resolve(); export async function main(){ return await Promise.resolve(42) }').value, 42);
  assert.equal(evaluateJavaScript(engine, {source:'const fn = () => 7; export { fn as calculate };', export:'calculate'}).value, 7);
});
test("guest VM has no host capabilities and does not retain globals", () => {
  assert.deepEqual(evaluate('export function main(){ globalThis.marker=1; return [typeof window, typeof document, typeof fetch, typeof navigator, typeof process, typeof require, typeof setTimeout, typeof WebAssembly]; }').value, Array(8).fill("undefined"));
  assert.equal(evaluate('export function main(){return typeof marker}').value, "undefined");
  assert.throws(() => evaluate('import x from "https://example.com/x.js"; export function main(){return x}'), {code:"javascript_exception"});
});
test("infinite loops and self-perpetuating promise jobs are interrupted", () => {
  assert.throws(() => evaluate('export function main(){while(true){}}'), {code:"javascript_timeout"});
  assert.throws(() => evaluate('export async function main(){while(true){await Promise.resolve()}}'), {code:"javascript_timeout"});
  assert.equal(evaluate('export function main(){return 9}').value, 9);
});
test("memory, source, input and output budgets cannot be raised by callers", () => {
  assert.throws(() => evaluate('export function main(){return new ArrayBuffer(32 * 1024 * 1024);}'), {category:"resource_exhausted"});
  assert.throws(() => evaluate('export function main(){return "x".repeat(300000)}'), {code:"javascript_output_too_large"});
  assert.throws(() => validateJavaScriptRequest({source:' '.repeat(65536)+'export function main(){}'}), {code:"javascript_source_too_large"});
  assert.throws(() => validateJavaScriptRequest({source:'export function main(){}',input:'x'.repeat(1100000)}), {code:"javascript_input_too_large"});
  assert.throws(() => validateJavaScriptRequest({source:'export function main(){}',timeoutMs:100000}), {code:"javascript_invalid_arguments"});
});
test("invalid programs, missing exports, rejected/pending promises and invalid results return errors", () => {
  for (const source of ['export function main( {', 'export function main(){throw new Error("boom")}', 'export async function main(){throw new Error("boom")}']) assert.throws(() => evaluate(source), {code:"javascript_exception"});
  assert.throws(() => evaluate('export const other = 1;'), {code:"javascript_export_not_found"});
  assert.throws(() => evaluate('export function main(){return new Promise(()=>{})}'), {code:"javascript_pending_promise"});
  assert.throws(() => evaluate('export function main(){}'), {code:"javascript_invalid_result"});
  assert.throws(() => evaluate('export function main(){const x={};x.x=x;return x}'), {code:"javascript_exception"});
});
test("console spam is bounded and VM changes cannot replace the captured result serializer", () => {
  const result=evaluate('export function main(){for(let i=0;i<1000;i++) console.log("x".repeat(1000)); JSON.stringify=()=>"false";return 12}');
  assert.equal(result.value,12);
  assert.equal(result.logs_truncated,true);
  assert.ok(result.logs.length<=100);
  assert.ok(result.logs.reduce((n,l)=>n+new TextEncoder().encode(l.text).length,0)<=16384);
});
test("cancelling code terminates the isolated execution Worker", async () => {
  let terminated=false;
  class FakeWorker extends EventTarget {postMessage(){} terminate(){terminated=true;}}
  globalThis.Worker=FakeWorker;
  try {
    const controller=new AbortController();
    const pending=executeJavaScript({source:'export function main(){while(true){}}'},controller.signal);
    controller.abort();
    await assert.rejects(pending,{code:"javascript_cancelled"});
    assert.equal(terminated,true);
  } finally {delete globalThis.Worker;}
});
