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
from urllib.error import URLError
from urllib.request import Request, urlopen


class FakeProvider(BaseHTTPRequestHandler):
    calls = 0

    def log_message(self, *_arguments):
        pass

    def do_POST(self):
        assert self.path == "/v1/responses"
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert request["model"] == "p5-fake"
        assert request["stream"] is True
        FakeProvider.calls += 1
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for event in [
            {"type": "response.created", "response": {"id": "fake"}},
            {"type": "response.output_text.delta", "delta": "hello "},
            {"type": "response.output_text.delta", "delta": "fake"},
            {"type": "response.completed", "response": {"id": "fake", "usage": {
                "input_tokens": 2, "output_tokens": 2, "total_tokens": 4,
            }}},
        ]:
            self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()


def frames(wire):
    events = []
    for frame in wire.split("\n\n"):
        fields = dict(line.split(": ", 1) for line in frame.splitlines() if ": " in line)
        if "data" not in fields:
            continue
        event = json.loads(fields["data"])
        assert int(fields["id"]) == event["seq"]
        assert fields["event"] == event["type"]
        events.append(event)
    return events


def stopped(port):
    with socket.socket() as probe:
        probe.settimeout(1)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def stop(process, port):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    assert stopped(port), f"port {port} still listening"


def main():
    binary = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug/mina-server").resolve()
    assert binary.is_file(), "build mina-server first"
    provider = ThreadingHTTPServer(("127.0.0.1", 0), FakeProvider)
    provider_port = provider.server_address[1]
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        server_port = reservation.getsockname()[1]
    base = f"http://127.0.0.1:{server_port}"
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix="mina-p5-smoke-") as directory:
            root = Path(directory)
            config = root / "config.toml"
            config.write_text(f'''default_model = "primary"
[agent]
kind = "agent-loop"
system_prompt = "Use only the fake response."
max_steps = 2
[script.quickjs]
enabled = false
[adf]
enabled = false
[jobs]
enabled = false
[models.primary]
model = "p5-fake"
[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:{provider_port}/v1"
protocol = "responses"
''')
            environment = dict(os.environ, MINA_CONFIG=str(config),
                MINA_RUN_STORE_PATH=str(root / "runs.db"), MINA_BLOB_STORE_PATH=str(root / "blobs"),
                MINA_SERVER_ADDR=f"127.0.0.1:{server_port}", MINA_ENGINE_PROFILE="operator_v1_tools",
                MINA_OPERATOR_BASIS_POINTS="10000", MINA_OPERATOR_ALLOWLIST="", RUST_LOG="warn")
            with (root / "server.log").open("w+") as log:
                def launch():
                    child = subprocess.Popen([str(binary)], cwd=root, env=environment, stdout=log, stderr=log)
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        if child.poll() is not None:
                            raise RuntimeError("server exited before readiness")
                        try:
                            with urlopen(base + "/healthz", timeout=1) as response:
                                assert response.status == 200
                            return child
                        except (URLError, TimeoutError):
                            time.sleep(0.05)
                    stop(child, server_port)
                    raise TimeoutError("server readiness")

                process = launch()
                request = Request(base + "/api/v1/runs", json.dumps({
                    "input": "fake smoke", "stream": True, "max_steps": 2,
                }).encode(), {"Content-Type": "application/json"})
                with urlopen(request, timeout=30) as response:
                    live = frames(response.read().decode())
                assert live[-1]["type"] == "run_completed"
                assert [event["seq"] for event in live] == list(range(1, len(live) + 1))
                run_id = live[0]["run_id"]
                events_url = base + f"/api/v1/runs/{run_id}/events?follow=false"
                stop(process, server_port)
                environment["MINA_ENGINE_PROFILE"] = "legacy"
                environment["MINA_OPERATOR_BASIS_POINTS"] = "0"
                process = launch()
                with urlopen(events_url, timeout=30) as response:
                    assert frames(response.read().decode()) == live
                with urlopen(Request(events_url, headers={"Last-Event-ID": "1"}), timeout=30) as response:
                    assert frames(response.read().decode()) == live[1:]
                assert FakeProvider.calls == 1
                request = Request(base + "/api/v1/runs", json.dumps({
                    "input": "legacy smoke", "stream": False, "max_steps": 2,
                }).encode(), {"Content-Type": "application/json"})
                with urlopen(request, timeout=30) as response:
                    result = json.load(response)
                assert result["output"] == "hello fake", result
                assert FakeProvider.calls == 2
                stop(process, server_port)
    finally:
        stop(process, server_port)
        provider.shutdown()
        provider.server_close()
        thread.join(timeout=10)
        assert stopped(provider_port)
    print(json.dumps({"http_sse": "passed", "rollback_replay": "passed",
        "provider_calls": FakeProvider.calls, "server_port": server_port,
        "provider_port": provider_port, "ports_stopped": True}))


if __name__ == "__main__":
    main()
