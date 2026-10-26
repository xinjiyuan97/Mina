use super::*;
use axum::body::to_bytes;
use serde_json::{Value, json};

const RUN: &str = "00000000-0000-0000-0000-000000000001";

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/p0")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).expect("P0 golden fixture must be frozen"))
        .expect("valid golden JSON")
}

async fn body(response: Response) -> String {
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(response.into_body(), 1024 * 1024),
    )
    .await
    .expect("response must finish")
    .expect("body");
    String::from_utf8(bytes.to_vec()).expect("UTF-8")
}

async fn json_body(response: Response) -> Value {
    serde_json::from_str(&body(response).await).expect("JSON response")
}

fn frames(text: &str) -> Vec<Value> {
    text.split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .map(|frame| {
            let mut id = None;
            let mut name = None;
            let mut payload = None;
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("id: ") {
                    id = Some(value.parse::<u64>().expect("numeric SSE id"));
                }
                if let Some(value) = line.strip_prefix("event: ") {
                    name = Some(value.to_owned());
                }
                if let Some(value) = line.strip_prefix("data: ") {
                    payload = Some(serde_json::from_str::<Value>(value).expect("event JSON"));
                }
            }
            let payload = payload.expect("data");
            assert_eq!(json!(id.expect("id")), payload["seq"]);
            assert_eq!(json!(name.as_ref().expect("name")), payload["type"]);
            json!({"id": id, "event": name, "data": payload})
        })
        .collect()
}

// Delta batching is not a wire guarantee. Keep exact cursor/replay checks above
// the golden comparison, and normalize only batching for the fixture itself.
fn normalize_output_batches(mut value: Value) -> Value {
    let mut normalized: Vec<Value> = Vec::new();
    for frame in value["stream"].as_array().expect("stream").iter().cloned() {
        if let Some(previous) = normalized.last_mut()
            && frame["event"] == "output_delta"
            && previous["event"] == "output_delta"
            && previous["data"]["channel"] == frame["data"]["channel"]
        {
            previous["data"]["delta"] = json!(format!(
                "{}{}",
                previous["data"]["delta"].as_str().expect("delta"),
                frame["data"]["delta"].as_str().expect("delta")
            ));
        } else {
            normalized.push(frame);
        }
    }
    for (index, frame) in normalized.iter_mut().enumerate() {
        frame["id"] = json!(index + 1);
        frame["data"]["seq"] = json!(index + 1);
    }
    value["stream"] = json!(normalized);
    value
}

async fn fake_state(
    gate: Option<Arc<tokio::sync::Notify>>,
) -> (AppState, tokio::task::JoinHandle<()>) {
    async fn responses(
        State(gate): State<Option<Arc<tokio::sync::Notify>>>,
        Json(request): Json<Value>,
    ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
        assert_eq!(request["model"], "p0-fake");
        assert_eq!(request["stream"], true);
        if let Some(gate) = gate {
            gate.notified().await;
        }
        Sse::new(futures_util::stream::iter([
            Ok(Event::default().data(r#"{"type":"response.created","response":{"id":"resp_p0"}}"#)),
            Ok(Event::default().data(r#"{"type":"response.output_text.delta","delta":"hel"}"#)),
            Ok(Event::default().data(r#"{"type":"response.output_text.delta","delta":"lo"}"#)),
            Ok(Event::default().data(r#"{"type":"response.completed","response":{"id":"resp_p0","usage":{"input_tokens":2,"output_tokens":2,"total_tokens":4}}}"#)),
        ]))
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake provider bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/responses", post(responses))
                .with_state(gate),
        )
        .await
        .expect("serve fake");
    });
    let config = HarnessConfig::from_toml_str(&format!(
        r#"
default_model = "primary"
[agent]
kind = "agent-loop"
system_prompt = "Be concise."
max_steps = 2
model_timeout_seconds = 5
[script.quickjs]
enabled = false
[adf]
enabled = false
[jobs]
enabled = false
[models.primary]
model = "p0-fake"
[models.primary.provider]
type = "openai-compatible"
base_url = "http://{address}/v1"
protocol = "responses"
api_key = "fake"
"#
    ))
    .expect("config");
    (
        tests::test_state_with_config(config, InMemoryApprovalBroker::default()),
        server,
    )
}

#[tokio::test]
async fn p0_runs_stream_nonstream_replay_follow_fixture() {
    let (state, provider) = fake_state(None).await;
    let request = serde_json::from_value(json!({"input":"hello"})).expect("request defaults");
    let response = create_run(State(state.clone()), Json(request)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut result = json_body(response).await;
    let run_id = result["run_id"].as_str().expect("run id").to_owned();
    result["run_id"] = json!(RUN);
    let request =
        serde_json::from_value(json!({"input":"hello", "stream":true})).expect("stream request");
    let response = create_run(State(state.clone()), Json(request)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(
        response.headers()["cache-control"],
        "no-cache, no-transform"
    );
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    let mut live = frames(&body(response).await);
    for frame in &mut live {
        frame["data"]["run_id"] = json!(RUN);
    }
    let mut headers = HeaderMap::new();
    headers.insert("last-event-id", "1".parse().expect("header"));
    let mut invalid_header = HeaderMap::new();
    invalid_header.insert("last-event-id", "not-numeric".parse().expect("header"));
    let mut replay_baseline = Vec::new();
    for (after_seq, header, follow, cutoff) in [
        (None, HeaderMap::new(), false, 0),
        (None, invalid_header, true, 0),
        (None, headers.clone(), false, 1),
        (Some(2), headers, true, 2),
        (Some(u64::MAX), HeaderMap::new(), false, u64::MAX),
    ] {
        let response = get_run_events(
            State(state.clone()),
            Path(run_id.clone()),
            Query(RunEventsQuery {
                after_seq,
                follow: Some(follow),
            }),
            header,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut replay = frames(&body(response).await);
        for frame in &mut replay {
            frame["data"]["run_id"] = json!(RUN);
        }
        if cutoff == 0 {
            replay_baseline = replay.clone();
        }
        assert_eq!(
            replay,
            replay_baseline
                .iter()
                .filter(|frame| frame["id"].as_u64().expect("id") > cutoff)
                .cloned()
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(live, replay_baseline);
    let actual = json!({"non_stream":result, "stream":live});
    eprintln!(
        "P0 RUN TRACE {}",
        serde_json::to_string_pretty(&actual).expect("trace")
    );
    provider.abort();
    assert_eq!(
        normalize_output_batches(actual),
        normalize_output_batches(fixture("runs.json"))
    );
}

#[tokio::test]
async fn p0_session_submit_idempotency_busy_and_cancel_fixture() {
    let (state, provider) = fake_state(Some(Arc::new(tokio::sync::Notify::new()))).await;
    let response = create_session(
        State(state.clone()),
        Json(serde_json::from_value(json!({})).expect("create defaults")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let session = json_body(response).await;
    let session_id = session["session_id"]
        .as_str()
        .expect("session id")
        .to_owned();
    let mut actual = Vec::new();
    let mut original_run = String::new();
    let mut fingerprint = Value::Null;
    for (input, key, revision) in [
        ("hello", "same", 0),
        ("hello", "same", 0),
        ("different", "same", 0),
        ("hello", "new", 1),
    ] {
        let request = serde_json::from_value(
            json!({"input":input,"expected_revision":revision,"idempotency_key":key}),
        )
        .expect("submit");
        let response = submit_session_run(
            State(state.clone()),
            Path(session_id.clone()),
            Json(request),
        )
        .await;
        let status = response.status().as_u16();
        let mut value = json_body(response).await;
        if status == 202 {
            if original_run.is_empty() {
                original_run = value["run_id"].as_str().expect("run").into();
                fingerprint = value["context_fingerprint"].clone();
                assert!(fingerprint.as_str().is_some_and(|value| !value.is_empty()));
            }
            assert_eq!(value["run_id"], original_run);
            assert_eq!(value["session_id"], session_id);
            assert_eq!(value["context_fingerprint"], fingerprint);
            value["run_id"] = json!(RUN);
            value["session_id"] = json!(RUN);
            value["context_fingerprint"] = json!("<context-fingerprint>");
        } else if let Some(message) = value["message"].as_str() {
            value["message"] = json!(
                message
                    .replace(&session_id, RUN)
                    .replace(&original_run, RUN)
            );
        }
        actual.push(json!({"status":status, "body":value}));
    }
    let response = cancel_run(State(state.clone()), Path(original_run.clone())).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let mut value = json_body(response).await;
    assert_eq!(value["run_id"], original_run);
    value["run_id"] = json!(RUN);
    actual.push(json!({"status":202,"body":value}));
    let response = get_run_events(
        State(state.clone()),
        Path(original_run.clone()),
        Query(RunEventsQuery::default()),
        HeaderMap::new(),
    )
    .await;
    let events = frames(&body(response).await);
    assert_eq!(events.last().expect("terminal")["event"], "run_cancelled");
    let response = cancel_run(State(state), Path(original_run)).await;
    actual.push(json!({"status":response.status().as_u16(),"body":json_body(response).await}));
    provider.abort();
    eprintln!(
        "P0 SESSION TRACE {}",
        serde_json::to_string_pretty(&actual).expect("trace")
    );
    assert_eq!(json!(actual), fixture("session-cancel.json"));
}

#[tokio::test]
async fn p0_all_sse_names_payloads_and_error_frame_fixture() {
    let golden = fixture("event-catalog.json");
    for entry in golden.as_array().expect("catalog") {
        let event: RunEvent = serde_json::from_value(entry["data"].clone()).expect("legacy event");
        assert_eq!(
            event.kind.event_name(),
            entry["event"].as_str().expect("name")
        );
        assert_eq!(
            serde_json::to_value(&event).expect("serialize"),
            entry["data"]
        );
        let response = Sse::new(futures_util::stream::iter([Ok::<_, Infallible>(
            sse_event(event),
        )]))
        .into_response();
        assert_eq!(frames(&body(response).await), vec![entry.clone()]);
    }
    let response = Sse::new(futures_util::stream::iter([Ok::<_, Infallible>(
        stream_error_event("failure"),
    )]))
    .into_response();
    assert_eq!(
        body(response).await,
        "event: run_stream_error\ndata: {\"code\":\"run_stream_error\",\"message\":\"failure\",\"retryable\":true}\n\n"
    );
}

#[tokio::test]
async fn p0_error_and_approval_fixtures() {
    let state = tests::test_state();
    let mut actual = Vec::new();
    for response in [
        cancel_run(State(state.clone()), Path("invalid".into())).await,
        cancel_run(State(state.clone()), Path(RUN.into())).await,
        submit_session_run(
            State(state.clone()),
            Path("invalid".into()),
            Json(
                serde_json::from_value(
                    json!({"input":"hello","expected_revision":0,"idempotency_key":"key"}),
                )
                .expect("request"),
            ),
        )
        .await,
        submit_session_run(
            State(state.clone()),
            Path(RUN.into()),
            Json(
                serde_json::from_value(
                    json!({"input":"hello","expected_revision":0,"idempotency_key":" "}),
                )
                .expect("request"),
            ),
        )
        .await,
        create_run(
            State(state),
            Json(serde_json::from_value(json!({"input":""})).expect("request")),
        )
        .await,
    ] {
        actual.push(json!({"status":response.status().as_u16(), "body":json_body(response).await}));
    }
    let broker = InMemoryApprovalBroker::default();
    let run_id = RUN.parse().expect("run");
    let approval_id = RUN.parse().expect("approval");
    let waiting = broker.request(ApprovalRequest {
        approval_id,
        run_id,
        call_id: "call".into(),
        tool_name: "tool".into(),
        risk_level: agent_core::tool::ToolRiskLevel::High,
        arguments: json!({}),
    });
    let state = tests::test_state_with_approvals(broker);
    for decision in [
        ApprovalDecision::Deny,
        ApprovalDecision::Deny,
        ApprovalDecision::AllowOnce,
    ] {
        let response = resolve_approval(
            State(state.clone()),
            Path((RUN.into(), RUN.into())),
            Json(ResolveApprovalRequest {
                decision,
                reason: None,
            }),
        )
        .await;
        actual.push(json!({"status":response.status().as_u16(), "body":json_body(response).await}));
    }
    assert_eq!(
        waiting.await.expect("resolved").decision,
        ApprovalDecision::Deny
    );
    eprintln!(
        "P0 ERROR TRACE {}",
        serde_json::to_string_pretty(&actual).expect("trace")
    );
    assert_eq!(json!(actual), fixture("errors-approval.json"));
}
