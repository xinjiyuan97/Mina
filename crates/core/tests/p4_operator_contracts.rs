use agent_core::{harness::ApprovalId, tool_authorization::approval_payload_schema};
use serde_json::json;

#[test]
fn pause_resume_control_is_revision_fenced_and_terminal_is_absorbing() {
    use agent_core::effect_pipeline::{RunControl, RunControlAction, RunControlPhase};
    let mut control = RunControl::default();
    assert!(control.apply(0, RunControlAction::Pause));
    assert_eq!(control.phase, RunControlPhase::Paused);
    assert!(!control.apply(0, RunControlAction::Resume));
    let restored: RunControl =
        serde_json::from_value(serde_json::to_value(&control).expect("fixture operation"))
            .expect("fixture operation");
    assert_eq!(control, restored);
    assert!(control.apply(1, RunControlAction::Resume));
    assert_eq!(control.phase, RunControlPhase::Running);
    assert!(control.apply(2, RunControlAction::Pause));
    assert!(control.apply(3, RunControlAction::Cancel));
    assert_eq!(control.phase, RunControlPhase::Terminal);
    assert!(!control.apply(4, RunControlAction::Resume));
    assert!(!control.apply(4, RunControlAction::Pause));
    assert!(!control.apply(4, RunControlAction::Complete));
    let mut completed = RunControl::default();
    assert!(completed.apply(0, RunControlAction::Complete));
    assert!(!completed.apply(1, RunControlAction::Cancel));
}

#[test]
fn approval_facts_require_bindings_and_resolved_requires_a_real_decision() {
    let digest = format!("sha256:{}", "0".repeat(64));
    for kind in [
        "approval.requested",
        "approval.resolved",
        "approval.expired",
        "approval.cancelled",
    ] {
        let schema = approval_payload_schema(kind);
        let validator = jsonschema::JSONSchema::compile(&schema).expect("schema");
        let mut payload = json!({
            "approval_id":ApprovalId::new(),"call_id":"call","effect_id":"effect",
            "arguments_digest":digest,"binding_digest":digest,"tool_set_revision":1,"tool_set_digest":digest,
            "actor":"gateway","resolution":null,"expires_at_ms":100,
        });
        if kind == "approval.resolved" {
            payload["resolution"] = json!({"decision":"allow-once"});
        }
        assert!(validator.is_valid(&payload), "{kind}");
        let mut missing = payload.clone();
        missing
            .as_object_mut()
            .expect("object")
            .remove("binding_digest");
        assert!(!validator.is_valid(&missing));
        payload["resolution"] = json!({});
        assert!(!validator.is_valid(&payload));
        payload["resolution"] = json!({"decision":"approve-everything"});
        assert!(!validator.is_valid(&payload));
    }
    assert_eq!(approval_payload_schema("approval.unknown"), json!(false));
}
