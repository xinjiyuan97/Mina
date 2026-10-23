use agent_core::{harness::RunId, pipeline::*};

fn item(sequence: u64, fragment: &str) -> DeltaItem {
    DeltaItem {
        invocation_id: "invocation".into(),
        delta_seq: sequence,
        channel: DeltaChannel::Text,
        fragment: fragment.into(),
    }
}

#[test]
fn single_batch_duplicates_reordering_and_repair_are_equivalent() {
    let journal = vec![item(1, "hel"), item(2, "lo"), item(3, "!")];
    let mut single = OutputBuffer::new("invocation".into(), 8, 1024);
    for delta in &journal {
        single.push(vec![delta.clone()]).expect("single");
    }
    let mut batch = OutputBuffer::new("invocation".into(), 8, 1024);
    batch
        .push(vec![
            journal[2].clone(),
            journal[0].clone(),
            journal[0].clone(),
        ])
        .expect("reorder");
    assert!(batch.has_gap());
    batch.repair(&journal).expect("repair");
    assert_eq!(
        single.finish().expect("single"),
        batch.finish().expect("batch")
    );
    assert!(batch.push(vec![item(1, "conflict")]).is_err());
    assert!(batch.push(vec![item(0, "bad")]).is_err());
}

#[test]
fn unrepaired_gap_and_lag_fail_explicitly() {
    let mut output = OutputBuffer::new("invocation".into(), 2, 8);
    output.push(vec![item(2, "tail")]).expect("gap");
    assert!(output.finish().is_err());
    assert!(output.push(vec![item(3, "oversize")]).is_err());
    assert!(output.repair(&[item(1, "bad")]).is_err());
}

#[test]
fn context_requests_once_fences_results_and_joins_terminal_in_both_orders() {
    for seal_first in [false, true] {
        let mut ledger = RunLedger::new(RunId::new(), "effect".into(), "invocation".into());
        let token = ledger.token.clone();
        assert_eq!(
            ledger
                .reduce(0, &token, Control::ContextCompleted)
                .expect("context"),
            vec![Intent::RunStarted, Intent::ModelRequested]
        );
        assert!(
            ledger
                .reduce(1, &token, Control::ContextCompleted)
                .expect("duplicate")
                .is_empty()
        );
        let mut stale = token.clone();
        stale.attempt += 1;
        assert!(ledger.reduce(1, &stale, Control::ModelStarted).is_err());
        stale = token.clone();
        stale.lease_epoch += 1;
        assert!(ledger.reduce(1, &stale, Control::ModelStarted).is_err());
        stale = token.clone();
        stale.issued_revision += 1;
        assert!(ledger.reduce(1, &stale, Control::ModelStarted).is_err());
        assert!(ledger.reduce(0, &token, Control::ModelStarted).is_err());
        let model = Control::ModelCompleted;
        let sealed = Control::OutputSealed;
        let (first, second) = if seal_first {
            (sealed, model)
        } else {
            (model, sealed)
        };
        assert!(ledger.reduce(1, &token, first).expect("first").is_empty());
        assert_eq!(
            ledger.reduce(2, &token, second.clone()).expect("second"),
            vec![Intent::RunCompleted]
        );
        assert!(
            ledger
                .reduce(3, &token, second)
                .expect("duplicate")
                .is_empty()
        );
        assert_eq!(ledger.phase, RunPhase::Terminal);
    }
}

#[test]
fn durable_contracts_reject_missing_fields_and_transient_shapes_are_equivalent() {
    for kind in [
        "model.requested",
        "model.started",
        "model.completed",
        "model.failed",
        "output.batch",
        "output.sealed",
        "output.failed",
        "run.completed",
        "run.failed",
    ] {
        let schema = payload_schema(kind);
        let validator = jsonschema::JSONSchema::compile(&schema).expect("schema");
        assert!(!validator.is_valid(&serde_json::json!({})), "{kind}");
    }
    assert_eq!(payload_schema("tool.requested"), serde_json::json!(false));
    let single = DeltaMessage::Single {
        item: item(1, "hello"),
    };
    let batch = DeltaMessage::Batch {
        items: vec![item(1, "hello")],
    };
    assert_eq!(
        serde_json::to_value(&single).expect("serialize")["event_type"],
        "model.delta"
    );
    assert_eq!(
        serde_json::to_value(&batch).expect("serialize")["event_type"],
        "model.delta.batch"
    );
    assert_eq!(single.into_items(), batch.into_items());
}
