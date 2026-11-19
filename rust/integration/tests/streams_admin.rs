// Service-owned.
//! Streams and admin modules against the real service.

use grow_openobserve as sdk;
use grow_openobserve_integration as support;
use sdk::admin::CreateServiceAccountOptions;
use sdk::logs::*;
use sdk::{ErrorCode, OpenObserveClient, OpenObserveConfig, StreamType};
use serde_json::json;
use support::*;

#[tokio::test]
async fn stream_lifecycle() {
    let c = root_client();
    let streams = c.streams();
    let stream = unique("streams");
    streams
        .create_stream(
            &stream,
            CreateStreamOptions {
                fields: vec![
                    SchemaField::new("message", "Utf8"),
                    SchemaField::new("run_id", "Utf8"),
                ],
                retention_days: 3,
                full_text_search_keys: vec!["message".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    c.logs()
        .ingest_json(
            &stream,
            &[json!({"message": "m", "run_id": "r", "level": "info"})],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    let listed = streams
        .list_streams(
            StreamType::Logs,
            ListStreamsOptions {
                keyword: Some(stream.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, stream);
    assert_eq!(listed[0].stream_type, "logs");
    let schema = streams
        .get_stream_schema(&stream, StreamType::Logs)
        .await
        .unwrap();
    assert!(schema
        .schema
        .iter()
        .any(|f| f.name == "message" && f.field_type == "Utf8"));
    assert_eq!(schema.settings.retention_days, 3);

    streams
        .update_stream_settings(
            &stream,
            StreamSettingsUpdate {
                retention_days: Some(7),
                partition_keys: Some(ListUpdate::Replace(vec!["run_id".into()])),
                full_text_search_keys: Some(ListUpdate::Replace(vec![
                    "message".into(),
                    "level".into(),
                ])),
                ..Default::default()
            },
            StreamType::Logs,
        )
        .await
        .unwrap();
    let after = streams
        .get_stream_schema(&stream, StreamType::Logs)
        .await
        .unwrap();
    assert_eq!(after.settings.retention_days, 7);
    assert!(after
        .settings
        .partition_keys
        .iter()
        .any(|k| k.field == "run_id" && !k.disabled));
    let mut fts = after.settings.full_text_search_keys.clone();
    fts.sort();
    assert_eq!(fts, ["level", "message"]);

    streams
        .update_stream_settings(
            &stream,
            StreamSettingsUpdate {
                partition_keys: Some(ListUpdate::Delta {
                    add: vec![],
                    remove: vec!["run_id".into()],
                }),
                full_text_search_keys: Some(ListUpdate::Delta {
                    add: vec![],
                    remove: vec!["level".into()],
                }),
                ..Default::default()
            },
            StreamType::Logs,
        )
        .await
        .unwrap();
    let removed = streams
        .get_stream_schema(&stream, StreamType::Logs)
        .await
        .unwrap();
    assert!(removed
        .settings
        .partition_keys
        .iter()
        .all(|k| k.field != "run_id" || k.disabled));
    assert_eq!(removed.settings.full_text_search_keys, ["message"]);

    streams
        .delete_stream(&stream, StreamType::Logs)
        .await
        .unwrap();
    assert_eq!(
        streams
            .delete_stream(&stream, StreamType::Logs)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        streams
            .get_stream_schema(&stream, StreamType::Logs)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        streams
            .update_stream_settings(
                &stream,
                StreamSettingsUpdate {
                    retention_days: Some(1),
                    ..Default::default()
                },
                StreamType::Logs
            )
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    let traces = streams
        .list_streams(StreamType::Traces, ListStreamsOptions::default())
        .await
        .unwrap();
    assert!(traces.iter().all(|s| s.stream_type == "traces"));
}

#[tokio::test]
async fn admin_service_accounts_and_passcode() {
    let c = root_client();
    let admin = c.admin();
    let orgs = admin.list_organizations().await.unwrap();
    assert!(orgs.iter().any(|o| o.identifier == "default"));

    let email = format!("{}@stack.local", unique("sa").replace('_', "-"));
    let created = admin
        .create_service_account(
            &email,
            CreateServiceAccountOptions {
                first_name: Some("Rust".into()),
                last_name: Some("SDK".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.user, email);
    let dup = admin
        .create_service_account(&email, Default::default())
        .await
        .unwrap_err();
    assert_eq!(dup.code(), ErrorCode::Conflict);
    let invalid = admin
        .create_service_account("not-an-email", Default::default())
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), ErrorCode::Validation);
    let listed = admin.list_service_accounts().await.unwrap();
    let account = listed.iter().find(|a| a.email == email).expect("listed");
    assert_eq!(account.first_name, "Rust");
    assert!(account.created_at.is_some());

    // the new token authenticates; after rotation only the new one does
    let as_sa = |token: &sdk::core::Secret| {
        OpenObserveClient::new(
            OpenObserveConfig::builder()
                .url(c.config().url().as_str())
                .user(&email)
                .token(token.clone())
                .max_retries(0)
                .build()
                .unwrap(),
        )
        .unwrap()
    };
    as_sa(&created.token).ping().await.unwrap();
    let rotated = admin.rotate_service_account_token(&email).await.unwrap();
    assert_ne!(rotated.token, created.token);
    as_sa(&rotated.token).ping().await.unwrap();
    assert_eq!(
        as_sa(&created.token).ping().await.unwrap_err().code(),
        ErrorCode::Authentication
    );

    // the passcode can ingest but not query
    let passcode = admin.get_passcode().await.unwrap();
    let ingest_only = OpenObserveClient::new(
        OpenObserveConfig::builder()
            .url(c.config().url().as_str())
            .user(&passcode.user)
            .password(passcode.passcode.clone())
            .max_retries(0)
            .build()
            .unwrap(),
    )
    .unwrap();
    let stream = unique("passcode");
    let res = ingest_only
        .logs()
        .ingest_json(
            &stream,
            &[json!({"message": "via passcode"})],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(res.successful, 1);
    assert_eq!(
        ingest_only.ping().await.unwrap_err().code(),
        ErrorCode::Authentication
    );
    c.streams()
        .delete_stream(&stream, StreamType::Logs)
        .await
        .unwrap();

    admin.delete_service_account(&email).await.unwrap();
    assert_eq!(
        admin
            .delete_service_account(&email)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
}
