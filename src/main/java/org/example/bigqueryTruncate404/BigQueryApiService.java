package org.example.bigqueryTruncate404;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import lombok.extern.java.Log;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;


@Log
public class BigQueryApiService {

    private final GoogleCredentials credentials;

    private final String bigQueryProjectId;

    public BigQueryApiService(String serviceAccountCredentials, String bigQueryProjectId) {
        Objects.requireNonNull(bigQueryProjectId, "bigQueryProjectId missing");
        Objects.requireNonNull(serviceAccountCredentials, "serviceAccountCredentials missing");

        this.bigQueryProjectId = bigQueryProjectId;

        try {
            credentials = ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(serviceAccountCredentials.getBytes())
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    BigQuery getBigQueryService() {
        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(bigQueryProjectId)
                .build()
                .getService();
    }

    public Table createTable(String datasetName, String tableName, Schema schema) {
        Objects.requireNonNull(datasetName, "missing datasetName");
        Objects.requireNonNull(tableName, "missing tableName");
        Objects.requireNonNull(schema, "missing schema");

        TableId tableId = TableId.of(datasetName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        return getBigQueryService().create(tableInfo);
    }

    public boolean deleteTable(String datasetName, String tableName) {
        Objects.requireNonNull(datasetName, "missing datasetName");
        Objects.requireNonNull(tableName, "missing tableName");

        return getBigQueryService().delete(
                TableId.of(bigQueryProjectId, datasetName, tableName)
        );
    }

    public Table getTable(String datasetName, String tableName) {
        Objects.requireNonNull(datasetName, "missing datasetName");
        Objects.requireNonNull(tableName, "missing tableName");

        return getBigQueryService().getTable(
                TableId.of(bigQueryProjectId, datasetName, tableName)
        );
    }

    public boolean insertRows(String datasetName, String tableName, List<InsertAllRequest.RowToInsert> rows) {
        boolean hasErrors = false;

        Table table = getTable(datasetName, tableName);
        Objects.requireNonNull(table, "table '" + tableName + "' not found");

        InsertAllResponse response = table.insert(rows);

        if (response.hasErrors()) {
            hasErrors = true;
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                log.log(Level.WARNING, "Response error: " + entry.getValue());
            }
        }

        return !hasErrors;
    }

    public void truncate(String datasetName, String tableName) throws InterruptedException {
        String sql = "truncate table " + bigQueryProjectId + "." + datasetName + "." + tableName;
        execute(sql);
    }

    private void execute(String query) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .setUseLegacySql(false)
                .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = getBigQueryService().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
    }

}