// Service-owned.
//! Streams and admin modules against recorded OpenObserve responses.

mod support;

use grow_openobserve as sdk;
use sdk::admin::CreateServiceAccountOptions;
use sdk::core::ErrorCode;
use sdk::logs::*;
use sdk::StreamType;
use serde_json::json;
use support::*;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn list_and_schema() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/streams"))
        .respond_with(respond("listStreams.logs"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/streams"))
        .respond_with(respond("listStreams.traces"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/streams/app/schema"))
        .respond_with(respond("getStreamSchema.after_settings"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/streams/nope/schema"))
        .respond_with(respond("getStreamSchema.404"))
        .mount(&server)
        .await;
    let streams = client(&server).streams();
    let list = streams
        .list_streams(StreamType::Logs, ListStreamsOptions::default())
        .await
        .unwrap();
    assert!(!list.is_empty());
    assert!(list.iter().all(|s| s.stream_type == "logs"));
    assert!(list[0].stats.doc_num > 0 || list[0].stats.created_at.is_some());
    let traces = streams
        .list_streams(
            StreamType::Traces,
            ListStreamsOptions {
                keyword: Some("sdk".into()),
                offset: Some(0),
                limit: Some(5),
            },
        )
        .await
        .unwrap();
    assert!(traces.iter().all(|s| s.stream_type == "traces"));
    let schema = streams
        .get_stream_schema("App", StreamType::Logs)
        .await
        .unwrap();
    assert!(schema.schema.iter().any(|f| f.name == "_timestamp"));
    assert!(schema.settings.retention_days > 0);
    assert!(!schema.settings.partition_keys.is_empty());
    assert!(schema.settings.raw.contains_key("partition_keys"));
    assert!(schema.total_fields > 0);
    let err = streams
        .get_stream_schema("nope", StreamType::Logs)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    let reqs = requests(&server).await;
    assert_eq!(query(&reqs[0], "type").as_deref(), Some("logs"));
    assert_eq!(query(&reqs[0], "sort").as_deref(), Some("name"));
    assert_eq!(query(&reqs[1], "keyword").as_deref(), Some("sdk"));
    assert_eq!(query(&reqs[1], "limit").as_deref(), Some("5"));
    assert_eq!(query(&reqs[1], "offset").as_deref(), Some("0"));
}

#[tokio::test]
async fn update_settings_replace_and_delta() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/default/streams/app/schema"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "app",
            "settings": {
                "partition_keys": {
                    "L0": {"field": "old", "types": "value", "disabled": false},
                    "L1": {"field": "gone", "types": "value", "disabled": true},
                    "L2": {"field": "keep", "types": {"hash": 8}}
                },
                "full_text_search_keys": ["message", "body"],
                "index_fields": [],
                "data_retention": 3
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/default/streams/app/settings"))
        .respond_with(respond("updateStreamSettings.ok"))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/default/streams/nope/settings"))
        .respond_with(respond("updateStreamSettings.404"))
        .mount(&server)
        .await;
    let streams = client(&server).streams();
    streams
        .update_stream_settings(
            "app",
            StreamSettingsUpdate {
                retention_days: Some(7),
                partition_keys: Some(ListUpdate::Replace(vec![
                    "keep".into(),
                    PartitionKeyInput {
                        field: "run_id".into(),
                        types: Some("hash".into()),
                    },
                ])),
                full_text_search_keys: Some(ListUpdate::Replace(vec!["message".into()])),
                index_fields: Some(ListUpdate::Delta {
                    add: vec!["level".into()],
                    remove: vec![],
                }),
                bloom_filter_fields: Some(ListUpdate::Replace(vec!["trace_id".into()])),
            },
            StreamType::Logs,
        )
        .await
        .unwrap();
    streams
        .update_stream_settings(
            "app",
            StreamSettingsUpdate {
                partition_keys: Some(ListUpdate::Delta {
                    add: vec![],
                    remove: vec![String::from("old").into()],
                }),
                ..Default::default()
            },
            StreamType::Logs,
        )
        .await
        .unwrap();
    let err = streams
        .update_stream_settings("nope", StreamSettingsUpdate::default(), StreamType::Logs)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    let reqs = requests(&server).await;
    assert_eq!(reqs[0].method.as_str(), "GET");
    let body = json_body(&reqs[1]);
    assert_eq!(body["data_retention"], 7);
    assert_eq!(
        body["partition_keys"],
        json!({
            "add": [{"field": "run_id", "types": "hash"}],
            "remove": [{"field": "old", "types": "value"}]
        })
    );
    assert_eq!(
        body["full_text_search_keys"],
        json!({"add": [], "remove": ["body"]})
    );
    assert_eq!(
        body["index_fields"],
        json!({"add": ["level"], "remove": []})
    );
    assert_eq!(
        body["bloom_filter_fields"],
        json!({"add": ["trace_id"], "remove": []})
    );
    // a delta-only update does not read the schema
    assert_eq!(reqs[2].method.as_str(), "PUT");
    assert_eq!(
        json_body(&reqs[2]),
        json!({"partition_keys": {"add": [], "remove": [{"field": "old", "types": "value"}]}})
    );
}

#[tokio::test]
async fn create_and_delete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/default/streams/app"))
        .respond_with(respond("createStream.ok"))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/default/streams/app"))
        .respond_with(respond("deleteStream.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/default/streams/app"))
        .respond_with(respond("deleteStream.404"))
        .mount(&server)
        .await;
    let streams = client(&server).streams();
    streams
        .create_stream(
            "app",
            CreateStreamOptions {
                fields: vec![SchemaField::new("message", "Utf8")],
                retention_days: 3,
                partition_keys: vec!["run_id".into()],
                full_text_search_keys: vec!["message".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    streams
        .delete_stream("app", StreamType::Logs)
        .await
        .unwrap();
    let err = streams
        .delete_stream("app", StreamType::Logs)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    assert_eq!(
        streams
            .delete_stream("", StreamType::Logs)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let reqs = requests(&server).await;
    assert_eq!(
        json_body(&reqs[0]),
        json!({
            "fields": [{"name": "message", "type": "Utf8"}],
            "settings": {
                "partition_keys": [{"field": "run_id", "types": "value"}],
                "index_fields": [],
                "full_text_search_keys": ["message"],
                "bloom_filter_fields": [],
                "data_retention": 3
            }
        })
    );
    assert_eq!(query(&reqs[1], "type").as_deref(), Some("logs"));
}

#[tokio::test]
async fn admin_helpers() {
    let server = MockServer::start().await;
    Mock::given(path("/api/organizations"))
        .respond_with(respond("listOrganizations.ok"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/service_accounts"))
        .respond_with(respond("createServiceAccount.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/service_accounts"))
        .respond_with(respond("createServiceAccount.400_duplicate"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/service_accounts"))
        .respond_with(respond("createServiceAccount.400_invalid_email"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/default/service_accounts"))
        .respond_with(respond("listServiceAccounts.ok"))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/default/service_accounts/sa%40example.com"))
        .respond_with(respond("rotateServiceAccountToken.ok"))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/default/service_accounts/sa%40example.com"))
        .respond_with(respond("deleteServiceAccount.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/default/service_accounts/sa%40example.com"))
        .respond_with(respond("deleteServiceAccount.404"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/passcode"))
        .respond_with(respond("getPasscode.ok"))
        .mount(&server)
        .await;
    let admin = client(&server).admin();
    let orgs = admin.list_organizations().await.unwrap();
    assert!(orgs.iter().any(|o| o.identifier == "default"));
    assert!(orgs.iter().any(|o| !o.org_type.is_empty()));
    let created = admin
        .create_service_account(
            " sa@example.com ",
            CreateServiceAccountOptions {
                first_name: Some("SDK".into()),
                last_name: None,
            },
        )
        .await
        .unwrap();
    assert!(!created.token.expose_secret().is_empty());
    assert!(!format!("{created:?}").contains(created.token.expose_secret()));
    assert!(created.user.contains('@'));
    let dup = admin
        .create_service_account("sa@example.com", Default::default())
        .await
        .unwrap_err();
    assert_eq!(dup.code(), ErrorCode::Conflict);
    let bad = admin
        .create_service_account("nope", Default::default())
        .await
        .unwrap_err();
    assert_eq!(bad.code(), ErrorCode::Validation);
    let list = admin.list_service_accounts().await.unwrap();
    assert!(!list.is_empty());
    assert_eq!(list[0].role, "service_account");
    assert!(list[0].created_at.is_some());
    let rotated = admin
        .rotate_service_account_token("sa@example.com")
        .await
        .unwrap();
    assert!(!rotated.token.is_empty());
    admin
        .delete_service_account("sa@example.com")
        .await
        .unwrap();
    assert_eq!(
        admin
            .delete_service_account("sa@example.com")
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    let passcode = admin.get_passcode().await.unwrap();
    assert!(!passcode.user.is_empty());
    assert!(!passcode.passcode.is_empty());
    for e in [
        admin
            .create_service_account(" ", Default::default())
            .await
            .unwrap_err(),
        admin.rotate_service_account_token("").await.unwrap_err(),
        admin.delete_service_account("").await.unwrap_err(),
    ] {
        assert_eq!(e.code(), ErrorCode::Validation);
    }
    let reqs = requests(&server).await;
    assert_eq!(
        json_body(&reqs[1]),
        json!({"email": "sa@example.com", "first_name": "SDK", "last_name": ""})
    );
    let rotate = reqs.iter().find(|r| r.method.as_str() == "PUT").unwrap();
    assert_eq!(query(rotate, "rotateToken").as_deref(), Some("true"));
    assert!(rotate.url.path().contains("sa%40example.com"));
}

#[tokio::test]
async fn admin_missing_fields() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/passcode"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": {}})))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/service_accounts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code": 200})))
        .mount(&server)
        .await;
    let admin = client(&server).admin();
    assert_eq!(
        admin.get_passcode().await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(
        admin
            .create_service_account("a@b.c", Default::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}
