# bigquery-truncate-404
Demonstrates that bigquery api returns 404 "Table is truncated." when insert right after truncate 

refers to https://stackoverflow.com/questions/70013949/bigquery-404-table-is-truncated-when-insert-right-after-truncate

The question is how one can determine the status "Table is truncated." of an existing table in order to avoid inserting and receiving 404. One must be able to detect this status and wait until this state is gone and table ready for insert.

## reproduce
1. in org.example.bigqueryTruncate404.BigQueryApiServiceTests.SERVICE_ACCOUNT_CREDENTIALS provide json credentials of gservice account
2. in org.example.bigqueryTruncate404.BigQueryApiServiceTests.BIGQUERY_PROJECT_ID set project id of bigquery project
3. assure that bigquery dataset defined in org.example.bigqueryTruncate404.BigQueryApiServiceTests.DATA_SET_NAME is available
4. run org.example.bigqueryTruncate404.BigQueryApiServiceTests.test_insertRows in order to create the table with some data (there must be a small timegap between create and truncate)
5. if 4. fails, run it again until it succeeds (sometimes insert right after creation results in 404 table not found - there must be a small timegap between create and and insert)
6. run org.example.bigqueryTruncate404.BigQueryApiServiceTests.test_truncate_insert, it will result in 404 Table is truncated.

## error message
```
com.google.cloud.bigquery.BigQueryException: Table is truncated.

{
  "code" : 404,
  "errors" : [ {
    "domain" : "global",
    "message" : "Table is truncated.",
    "reason" : "notFound"
  } ],
  "message" : "Table is truncated.",
  "status" : "NOT_FOUND"
}
```