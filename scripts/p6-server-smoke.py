import base64
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from importlib.util import module_from_spec, spec_from_file_location

sys.dont_write_bytecode = True
spec = spec_from_file_location("p5_smoke", Path(__file__).with_name("p5-server-smoke.py"))
p5 = module_from_spec(spec)
spec.loader.exec_module(p5)
PAYLOAD = b"P6_PRIVATE_BINARY_SENTINEL"


class FakeProvider(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, *_arguments):
        pass

    def do_POST(self):
        assert self.path == "/v1/responses"
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert body["model"] == "p6-fake"
        FakeProvider.requests.append(body)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for event in [
            {"type": "response.created", "response": {"id": "fake"}},
            {"type": "response.output_text.delta", "delta": "fake reply"},
            {"type": "response.completed", "response": {"id": "fake", "usage": {
                "input_tokens": 2, "output_tokens": 2, "total_tokens": 4}}},
        ]:
            self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()


def main():
    binary = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug/mina-server").resolve()
    provider = ThreadingHTTPServer(("127.0.0.1", 0), FakeProvider)
    provider_port = provider.server_address[1]
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix="mina-p6-smoke-") as directory:
            root = Path(directory)
            config = root / "config.toml"
            config.write_text(f'''default_model = "primary"
[agent]
kind = "agent-loop"
system_prompt = "Use only fake responses."
max_steps = 2
[script.quickjs]
enabled = false
[adf]
enabled = false
[jobs]
enabled = false
[models.primary]
model = "p6-fake"
[models.primary.modalities]
input = ["text", "image", "audio", "video", "document"]
[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:{provider_port}/v1"
protocol = "responses"
''')
            environment = dict(os.environ, MINA_CONFIG=str(config),
                MINA_RUN_STORE_PATH=str(root / "runs.db"), MINA_BLOB_STORE_PATH=str(root / "blobs"),
                MINA_SERVER_ADDR=f"127.0.0.1:{port}", MINA_ENGINE_PROFILE="operator_v1_tools",
                MINA_OPERATOR_BASIS_POINTS="10000", MINA_OPERATOR_ALLOWLIST="", RUST_LOG="info")
            with (root / "server.log").open("w+") as log:
                def launch():
                    child = subprocess.Popen([str(binary)], cwd=root, env=environment, stdout=log, stderr=log)
                    try:
                        deadline = time.monotonic() + 30
                        while time.monotonic() < deadline:
                            assert child.poll() is None, "server exited"
                            try:
                                with urlopen(base + "/healthz", timeout=1):
                                    return child
                            except (URLError, TimeoutError):
                                time.sleep(0.05)
                        raise TimeoutError("readiness")
                    except BaseException:
                        p5.stop(child, port)
                        raise

                def request(path, data=None, headers=None):
                    if isinstance(data, dict):
                        data = json.dumps(data).encode()
                        headers = {"Content-Type": "application/json", **(headers or {})}
                    with urlopen(Request(base + path, data, headers or {}), timeout=30) as response:
                        return response.read()

                process = launch()
                attachments = []
                for mime, name in [("image/png", "private-image.png"), ("application/pdf", "private-doc.pdf"),
                                   ("audio/wav", "private-audio.wav"), ("video/mp4", "private-video.mp4"),
                                   ("application/octet-stream", "private-file.bin")]:
                    metadata = json.loads(request(f"/api/v1/blobs?name={name}", PAYLOAD, {
                        "Content-Type": mime}))
                    attachments.append(metadata)
                live = p5.frames(request("/api/v1/runs", {"input": "", "attachments": attachments, "stream": True}).decode())
                assert live[-1]["type"] == "run_completed", live
                content = FakeProvider.requests[-1]["input"][-1]["content"]
                assert [part["type"] for part in content] == ["input_image"] + ["input_file"] * 4
                assert all(base64.b64encode(PAYLOAD).decode() in json.dumps(part) for part in content)
                run_id = live[0]["run_id"]
                process.kill()
                process.wait(timeout=10)
                assert p5.stopped(port)
                process = launch()
                replay = p5.frames(request(f"/api/v1/runs/{run_id}/events?follow=false").decode())
                assert replay == live
                session = json.loads(request("/api/v1/sessions", {}))
                session_id = session["session_id"]
                first = {"input": "", "attachments": attachments[:2], "idempotency_key": "p6-first", "expected_revision": session["revision"]}
                accepted = json.loads(request(f"/api/v1/sessions/{session_id}/runs", first))
                same = json.loads(request(f"/api/v1/sessions/{session_id}/runs", first))
                assert same["run_id"] == accepted["run_id"] and same["replayed"]
                request(f"/api/v1/runs/{accepted['run_id']}/events?follow=true")
                deadline = time.monotonic() + 10
                while True:
                    session = json.loads(request(f"/api/v1/sessions/{session_id}"))
                    if not session.get("active_run_id"):
                        break
                    assert time.monotonic() < deadline, "session finalization"
                    time.sleep(0.05)
                second = json.loads(request(f"/api/v1/sessions/{session_id}/runs", {
                    "input": "remember the attachments", "idempotency_key": "p6-second", "expected_revision": session["revision"]}))
                request(f"/api/v1/runs/{second['run_id']}/events?follow=true")
                assert any(any(part["type"] == "input_image" for part in item.get("content", [])) for item in FakeProvider.requests[-1]["input"])
                for field, value in [("name", "renamed.png"), ("media_type", "image/jpeg")]:
                    changed = json.loads(json.dumps(first))
                    changed["attachments"][0][field] = value
                    try:
                        request(f"/api/v1/sessions/{session_id}/runs", changed)
                        raise AssertionError("changed identity accepted")
                    except HTTPError as error:
                        assert error.code == 409
                wire = request(f"/api/v1/sessions/{session_id}/messages")
                assert b"blob_ref" in wire
                assert PAYLOAD not in wire and base64.b64encode(PAYLOAD) not in wire
                p5.stop(process, port)
                environment["MINA_ENGINE_PROFILE"] = "legacy"
                environment["MINA_OPERATOR_BASIS_POINTS"] = "0"
                process = launch()
                legacy = json.loads(request("/api/v1/runs", {"input": "legacy", "attachments": attachments[:1], "stream": False}))
                assert legacy["output"] == "fake reply"
                p5.stop(process, port)
                for file in root.rglob("*"):
                    if file.is_file() and "blobs" not in file.parts:
                        stored = file.read_bytes()
                        assert PAYLOAD not in stored and base64.b64encode(PAYLOAD) not in stored, file.name
                log.seek(0)
                assert "private-image.png" not in log.read()
    finally:
        p5.stop(process, port)
        provider.shutdown()
        provider.server_close()
        thread.join(timeout=10)
        assert p5.stopped(provider_port)
    print(json.dumps({"multimodal_http_sse": "passed", "session_history": "passed", "legacy": "passed",
                      "durable_binary_scan": "passed", "provider_calls": len(FakeProvider.requests),
                      "server_port": port, "provider_port": provider_port, "ports_stopped": True}))


if __name__ == "__main__":
    main()
