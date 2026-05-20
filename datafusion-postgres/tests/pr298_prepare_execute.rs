use bytes::Buf;
use futures::StreamExt;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;

use datafusion_postgres::testing::*;
use datafusion_postgres::DfSessionService;

fn tag(response: &Response) -> String {
    match response {
        Response::Execution(tag) => pgwire::messages::response::CommandComplete::from(tag.clone()).tag,
        other => panic!("expected execution response, got {other:?}"),
    }
}

async fn first_text_value(mut response: Response) -> String {
    let Response::Query(ref mut qr) = response else {
        panic!("expected query response, got {response:?}");
    };
    let row = qr
        .data_rows()
        .next()
        .await
        .expect("expected one row")
        .expect("row should decode");

    let mut data = row.data.clone();
    let len = data.get_i32();
    assert!(len >= 0, "expected non-null first column");
    let bytes = data.copy_to_bytes(len as usize);
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn run_ok(service: &DfSessionService, client: &mut MockClient, sql: &str) -> Response {
    let mut responses = <DfSessionService as SimpleQueryHandler>::do_query(service, client, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e:?}"));
    assert_eq!(responses.len(), 1, "{sql}");
    responses.remove(0)
}

async fn run_err(service: &DfSessionService, client: &mut MockClient, sql: &str) -> String {
    let err = <DfSessionService as SimpleQueryHandler>::do_query(service, client, sql)
        .await
        .expect_err("expected error");
    format!("{err:?}")
}

#[tokio::test]
async fn test_pr298_psql_script_prepare_execute_deallocate() {
    let service = setup_handlers();
    let mut client = MockClient::new();

    assert_eq!(tag(&run_ok(&service, &mut client, "PREPARE my_stmt AS SELECT 42 AS answer").await), "PREPARE");
    assert_eq!(first_text_value(run_ok(&service, &mut client, "EXECUTE my_stmt").await).await, "42");
    assert_eq!(first_text_value(run_ok(&service, &mut client, "EXECUTE my_stmt").await).await, "42");

    assert_eq!(tag(&run_ok(&service, &mut client, "PREPARE param_stmt (INT) AS SELECT $1 * 2 AS doubled").await), "PREPARE");
    assert_eq!(first_text_value(run_ok(&service, &mut client, "EXECUTE param_stmt(21)").await).await, "42");
    assert_eq!(first_text_value(run_ok(&service, &mut client, "EXECUTE param_stmt(100)").await).await, "200");

    assert_eq!(tag(&run_ok(&service, &mut client, "DEALLOCATE my_stmt").await), "DEALLOCATE");
    assert_eq!(first_text_value(run_ok(&service, &mut client, "EXECUTE param_stmt(5)").await).await, "10");

    assert_eq!(tag(&run_ok(&service, &mut client, "PREPARE stmt_a AS SELECT 1").await), "PREPARE");
    assert_eq!(tag(&run_ok(&service, &mut client, "PREPARE stmt_b AS SELECT 2").await), "PREPARE");
    assert_eq!(tag(&run_ok(&service, &mut client, "DEALLOCATE ALL").await), "DEALLOCATE");

    for sql in ["EXECUTE stmt_a", "EXECUTE stmt_b", "EXECUTE param_stmt(1)", "EXECUTE never_prepared"] {
        let err = run_err(&service, &mut client, sql).await;
        assert!(err.contains("does not exist"), "{sql}: {err}");
    }
}
