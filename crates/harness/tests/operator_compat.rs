use agent_core::{
    event_runtime::{EventEnvelope, PublishEvent},
    harness::{AgentLoopEffect, RunEvent},
    operator::{LogSequence, PublicRunSequence},
};
use agent_harness::operator_compat::*;
use serde_json::json;

#[test]
fn every_reducer_effect_has_an_offline_target_and_phase() {
    let run = "00000000-0000-0000-0000-000000000001";
    let call = json!({"call_id":"call", "name":"tool", "arguments":{}, "public_arguments":{}, "risk_level":"low", "execution":{}});
    let cases = [
        (
            json!({"type":"load_tool_set", "run_id":run, "allow_run_adf":false}),
            "toolset",
            "toolset.requested",
            "AwaitingToolSet",
        ),
        (
            json!({"type":"invoke_model", "purpose":"agent_step", "request":{"run_id":run,"model":"fake","messages":[],"tools":[],"max_output_tokens":null},"timeout_ms":100}),
            "model",
            "model.requested",
            "AwaitingModel",
        ),
        (
            json!({"type":"validate_tools", "run_id":run,"tool_set_revision":1,"calls":[]}),
            "validator",
            "tool.validation.requested",
            "AwaitingValidation",
        ),
        (
            json!({"type":"request_approval", "run_id":run,"approval_id":run,"call":call}),
            "approval",
            "approval.requested",
            "AwaitingApproval",
        ),
        (
            json!({"type":"invoke_tool", "run_id":run,"tool_set_revision":1,"call":call,"timeout_ms":100}),
            "tool",
            "tool.requested",
            "AwaitingTools",
        ),
    ];
    for (mut value, operator, event_type, phase) in cases {
        value["effect_id"] = json!("stable-effect");
        let effect: AgentLoopEffect = serde_json::from_value(value).expect("legacy effect");
        let before = serde_json::to_value(&effect).expect("serialize");
        assert_eq!(
            effect_target(&effect),
            EffectTarget {
                effect_id: "stable-effect",
                operator,
                event_type,
                legacy_phase: phase
            }
        );
        assert_eq!(serde_json::to_value(effect).expect("serialize"), before);
    }
}

#[test]
fn legacy_publish_keeps_names_payload_and_only_storage_sequence() {
    let value = json!({"event_id":"00000000-0000-0000-0000-000000000001","topic":"existing.wait","event_type":"timer.fired","schema_version":1,"source":"timer","subject":"wait-key","correlation_id":"correlation","causation_id":null,"occurred_at_ms":1,"recorded_at_ms":2,"payload":{"timer":"unchanged"}});
    let event: PublishEvent = serde_json::from_value(value.clone()).expect("legacy publish");
    let view = publish_view(&event);
    assert_eq!(view.log_sequence, None);
    assert_eq!(
        serde_json::to_value(view.original).expect("serialize"),
        value
    );
    let stored = EventEnvelope {
        sequence: 81,
        event,
    };
    let view = stored_event_view(&stored);
    assert_eq!(view.log_sequence, Some(LogSequence(81)));
    assert_eq!(
        serde_json::to_value(view.original).expect("serialize"),
        value
    );
}

#[test]
fn public_event_sequence_is_not_an_operator_or_delta_sequence() {
    let value = json!({"run_id":"00000000-0000-0000-0000-000000000001","seq":42,"type":"output_delta","channel":"assistant_text","delta":"hello"});
    let event: RunEvent = serde_json::from_value(value.clone()).expect("public event");
    let view = public_event_view(&event);
    assert_eq!(view.sequence, PublicRunSequence(42));
    assert_eq!(view.sse_event_name, "output_delta");
    assert_eq!(
        serde_json::to_value(view.original).expect("serialize"),
        value
    );
}
