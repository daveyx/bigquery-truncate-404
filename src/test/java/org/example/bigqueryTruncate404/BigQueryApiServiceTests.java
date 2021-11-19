package org.example.bigqueryTruncate404;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class BigQueryApiServiceTests {


    private static final String SERVICE_ACCOUNT_CREDENTIALS = "<SERVICE_ACCOUNT_CREDENTIALS>";
    private static final String BIGQUERY_PROJECT_ID         = "<BIGQUERY_PROJECT_ID>";
    private static final String DATA_SET_NAME               = "my_dataset";
    private static final String TABLE_NAME                  = "test_table";

    private final BigQueryApiService bigQueryApiService = new BigQueryApiService(SERVICE_ACCOUNT_CREDENTIALS, BIGQUERY_PROJECT_ID);


    @Test
    void test_truncate_insert() throws InterruptedException {
        Objects.requireNonNull(getTestTable(), "test table must be present for this test");

        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> rows.add((InsertAllRequest.RowToInsert.of(createRow(index)))));

        bigQueryApiService.truncate(DATA_SET_NAME, TABLE_NAME);

        bigQueryApiService.insertRows(DATA_SET_NAME, TABLE_NAME, rows);
    }

    @Test
    public void test_insertRows() {
        createTestTableIfNotExists();

        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> rows.add((InsertAllRequest.RowToInsert.of(createRow(index)))));

        assertTrue(bigQueryApiService.insertRows(DATA_SET_NAME, TABLE_NAME, rows));
    }

    private Map<String, Object> createRow(int count) {
        Map<String, Object> row = new HashMap<>();
        row.put("idField", count);
        row.put("stringField", "a test string");
        row.put("booleanField", Boolean.TRUE);
        row.put("numericField", count + 1);
        row.put("bigNumericField", count + 2);
        row.put("int64Field", count + 3);
        row.put("timestampField", LocalDateTime.now().toString());

        return row;
    }

    @Test
    public void test_createTable() {
        Table table = createTestTable();

        assertNotNull(table.getCreationTime());
    }

    private Table createTestTableIfNotExists() {
        Table table = getTestTable();

        if (table == null) {
            return createTestTable();
        }

        return table;
    }

    private Table createTestTable() {
        Schema schema = Schema.of(
                Field.newBuilder("idField", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.of("stringField", StandardSQLTypeName.STRING),
                Field.of("booleanField", StandardSQLTypeName.BOOL),
                Field.of("numericField", StandardSQLTypeName.NUMERIC),
                Field.of("bigNumericField", StandardSQLTypeName.BIGNUMERIC),
                Field.of("int64Field", StandardSQLTypeName.INT64),
                Field.of("timestampField", StandardSQLTypeName.TIMESTAMP)
        );

        return bigQueryApiService.createTable(
                DATA_SET_NAME,
                TABLE_NAME,
                schema
        );
    }

    private Table getTestTable() {
        return bigQueryApiService.getTable(
                DATA_SET_NAME,
                TABLE_NAME
        );
    }

    @Test
    public void test_deleteTable() {
        boolean deleted = deleteTestTable();

        assertTrue(deleted);
    }

    private boolean deleteTestTable() {
        return bigQueryApiService.deleteTable(
                DATA_SET_NAME,
                TABLE_NAME
        );
    }

}
