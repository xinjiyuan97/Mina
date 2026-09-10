import assert from "node:assert/strict";
import test from "node:test";
import { BrowserToolRegistry } from "../dist/internal/browser-tools.js";
import { BrowserFileStore } from "../dist/internal/opfs-files.js";

test("image tools accept content inputs only and retain fixed transport settings", async () => {
  const config = {protocol:"responses", baseUrl:"https://images.example.test/v1/", apiKey:"test-key", model:"chat-model", organization:"test-org"};
  const registry = new BrowserToolRegistry(config);
  const definitions = registry.snapshot().definitions;
  assert.deepEqual(Object.keys(definitions.find(d=>d.name==="generate_image").input_schema.properties), ["prompt"]);
  assert.deepEqual(Object.keys(definitions.find(d=>d.name==="edit_image").input_schema.properties), ["source_path","prompt","mask_path"]);
  for (const name of ["generate_image", "edit_image"]) {
    const args = name === "generate_image" ? {prompt:"a tree"} : {prompt:"blue sky",source_path:"source.png"};
    assert.equal(registry.validate(name,args),null);
    for (const key of ["model","baseUrl","apiKey","api","size","quality","path","output_format"]) {
      assert.equal(registry.validate(name,{...args,[key]:"override"}).code,"invalid_tool_arguments");
    }
  }
  const saved=[], requests=[];
  const originalFetch=globalThis.fetch, originalRead=BrowserFileStore.prototype.readBinary, originalSave=BrowserFileStore.prototype.saveBinary;
  BrowserFileStore.prototype.readBinary=async path=>({blob:new Blob(["image"],{type:"image/png"}),file:{name:path}});
  BrowserFileStore.prototype.saveBinary=async (path,bytes,mediaType)=>{saved.push({path,bytes,mediaType}); return {path,name:path,kind:"file",size:bytes.length,mediaType,lastModified:0};};
  globalThis.fetch=async (url,options)=>{requests.push({url:String(url),...options}); return new Response(JSON.stringify({data:[{b64_json:"aW1hZ2U="}]}),{status:200});};
  try {
    const signal=new AbortController().signal;
    assert.equal((await registry.invoke("generate_image",{prompt:"a tree"},signal)).type,"completed");
    assert.equal((await registry.invoke("edit_image",{prompt:"blue sky",source_path:"source.png",mask_path:"mask.png"},signal)).type,"completed");
    assert.equal(requests[0].url,"https://images.example.test/v1/images/generations");
    assert.deepEqual(JSON.parse(requests[0].body),{model:"gpt-image-2",prompt:"a tree",size:"auto",quality:"auto",background:"auto",output_format:"png",n:1});
    assert.equal(requests[1].url,"https://images.example.test/v1/images/edits");
    assert.equal(requests[1].body.get("model"),"gpt-image-2");
    assert.equal(requests[1].body.get("prompt"),"blue sky");
    assert.equal(requests[1].body.get("output_format"),"png");
    assert.ok(requests[1].body.get("image[]") instanceof Blob);
    assert.ok(requests[1].body.get("mask") instanceof Blob);
    for(const request of requests) {
      assert.equal(request.headers.Authorization,"Bearer test-key");
      assert.equal(request.headers["OpenAI-Organization"],"test-org");
      assert.equal(request.signal,signal);
    }
    assert.match(saved[0].path,/^images\/generated-.*\.png$/);
    assert.match(saved[1].path,/^images\/edited-.*\.png$/);
    assert.ok(saved.every(x=>x.mediaType==="image/png"));
  } finally {
    globalThis.fetch=originalFetch;
    BrowserFileStore.prototype.readBinary=originalRead;
    BrowserFileStore.prototype.saveBinary=originalSave;
  }
});
