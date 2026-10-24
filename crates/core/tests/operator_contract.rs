use std::any::TypeId;

use agent_core::operator::*;
use serde_json::{Value, json};

#[test]
fn p5_manifest_reader_accepts_historical_versions_and_rejects_unknowns() {
    for manifest in [
        json!({}),
        json!({"engine_version":"legacy_machine"}),
        json!({"engine_version":"operator_v1", "projection_version":1}),
        json!({"engine_version":"operator_v1", "projection_version":1,"profile":"operator_v1_tools"}),
    ] {
        assert!(validate_execution_manifest(&manifest).is_ok());
    }
    for manifest in [
        json!({"engine_version":"operator_v2"}),
        json!({"engine_version":"operator_v1","projection_version":2}),
        json!({"engine_version":"operator_v1","projection_version":1,"profile":"unknown"}),
        json!({"engine_version":null}),
        json!({"projection_version":999}),
        json!({"schema_version":999}),
    ] {
        assert_eq!(
            validate_execution_manifest(&manifest),
            Err("unsupported_run_version")
        );
    }
}

fn wire() -> Value {
    json!({
        "schema_version": 1, "engine_version": "operator_v1", "projection_version": 1,
        "event_id": "00000000-0000-0000-0000-000000000001", "event_type": "model.completed",
        "tenant_id": "tenant", "run_id": "00000000-0000-0000-0000-000000000002", "session_id": null,
        "effect_id": "effect-1", "invocation_id": "invocation-1", "attempt": 1,
        "issued_revision": 0, "lease_epoch": 1, "correlation_id": "run-correlation", "causation_id": null,
        "operator": {"name": "model", "version": 1}, "operator_seq": 1,
        "delivery_class": "durable", "occurred_at_ms": 123, "payload": {"text": "你好"}
    })
}

fn contract() -> PayloadContract {
    PayloadContract {
        event_type: "model.completed".into(),
        delivery_class: DeliveryClass::Durable,
        schema: json!({"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}, "additionalProperties": false}),
        max_payload_bytes: 17,
    }
}

#[test]
fn sequence_domains_are_distinct() {
    let types = [
        TypeId::of::<OperatorSequence>(),
        TypeId::of::<DeltaSequence>(),
        TypeId::of::<PublicRunSequence>(),
        TypeId::of::<LogSequence>(),
        TypeId::of::<RunRevision>(),
        TypeId::of::<LeaseEpoch>(),
    ];
    for (index, domain) in types.iter().enumerate() {
        assert!(
            !types[index + 1..].contains(domain),
            "sequence domains must not alias"
        );
    }
}

#[test]
fn stable_wire_roundtrip_and_exact_utf8_limit() {
    let envelope: OperatorEnvelope = serde_json::from_value(wire()).expect("valid envelope");
    assert_eq!(envelope.validate(&contract()), Ok(()));
    assert_eq!(serde_json::to_value(&envelope).expect("serialize"), wire());
    let encoded = serde_json::to_string(&envelope).expect("encode");
    let decoded: OperatorEnvelope = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(serde_json::to_string(&decoded).expect("reencode"), encoded);
    let mut smaller = contract();
    smaller.max_payload_bytes = 16;
    assert_eq!(
        envelope.validate(&smaller),
        Err(ContractError::PayloadTooLarge)
    );
}

#[test]
fn rejects_unknown_versions_and_invalid_identity() {
    for field in ["schema_version", "projection_version"] {
        for version in [0, 2] {
            let mut value = wire();
            value[field] = json!(version);
            let envelope: OperatorEnvelope =
                serde_json::from_value(value).expect("decode future version");
            assert_eq!(
                envelope.validate(&contract()),
                Err(ContractError::UnsupportedVersion)
            );
        }
    }
    let mut value = wire();
    value["engine_version"] = json!("operator_v2");
    assert!(serde_json::from_value::<OperatorEnvelope>(value).is_err());
    for field in ["tenant_id", "effect_id", "invocation_id", "correlation_id"] {
        let mut value = wire();
        value[field] = json!(" ");
        let envelope: OperatorEnvelope = serde_json::from_value(value).expect("decode");
        assert_eq!(
            envelope.validate(&contract()),
            Err(ContractError::InvalidIdentity)
        );
    }
    for field in ["attempt", "lease_epoch", "operator_seq"] {
        let mut value = wire();
        value[field] = json!(0);
        let envelope: OperatorEnvelope = serde_json::from_value(value).expect("decode");
        assert_eq!(
            envelope.validate(&contract()),
            Err(ContractError::InvalidIdentity)
        );
    }
}

#[test]
fn rejects_schema_and_delivery_mismatch() {
    let mut envelope: OperatorEnvelope = serde_json::from_value(wire()).expect("decode");
    envelope.payload = json!({"text": 7});
    assert_eq!(
        envelope.validate(&contract()),
        Err(ContractError::InvalidPayload)
    );
    envelope.payload = json!({});
    assert_eq!(
        envelope.validate(&contract()),
        Err(ContractError::InvalidPayload)
    );
    envelope.payload = json!({"text": "ok"});
    let mut invalid = contract();
    invalid.schema = json!({"type": "not-a-type"});
    assert_eq!(
        envelope.validate(&invalid),
        Err(ContractError::InvalidSchema)
    );
    envelope.delivery_class = DeliveryClass::Transient;
    assert_eq!(
        envelope.validate(&contract()),
        Err(ContractError::ContractMismatch)
    );
    envelope.delivery_class = DeliveryClass::Durable;
    envelope.event_type = "tool.completed".into();
    assert_eq!(
        envelope.validate(&contract()),
        Err(ContractError::ContractMismatch)
    );
}
