package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpecialTypeHandlerTest {
    private SpecialTypeHandler handler;

    private static Stream<Arguments> provideMiscArrayData() {
        return Stream.of(
                // XMLARRAY cases
                Arguments.of(PostgresDataType.XMLARRAY,
                        "{\"<root><child>Test1</child></root>\",\"<root><child>Test2</child></root>\"}",
                        Arrays.asList("<root><child>Test1</child></root>", "<root><child>Test2</child></root>")),
                Arguments.of(PostgresDataType.XMLARRAY, "{}", Collections.emptyList()),

                // PG_LSNARRAY cases
                Arguments.of(PostgresDataType.PG_LSNARRAY,
                        "{16/B374D848,16/B374D849}",
                        Arrays.asList("16/B374D848", "16/B374D849")),
                Arguments.of(PostgresDataType.PG_LSNARRAY, "{}", Collections.emptyList()),

                // UUIDARRAY cases
                Arguments.of(PostgresDataType.UUIDARRAY,
                        "{123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001}",
                        Arrays.asList("123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001")),
                Arguments.of(PostgresDataType.UUIDARRAY, "{}", Collections.emptyList()),

                // TSVECTORARRAY cases
                Arguments.of(PostgresDataType.TSVECTORARRAY,
                        "{\"'word1':1 'word2':2,4,5 'word3':3,6,7\",\"'1':2 '2':6 '3':3 '4':7 '5':4 'word1':1 'word2':5\"}",
                        Arrays.asList("'word1':1 'word2':2,4,5 'word3':3,6,7", "'1':2 '2':6 '3':3 '4':7 '5':4 'word1':1 'word2':5")),
                Arguments.of(PostgresDataType.TSVECTORARRAY, "{}", Collections.emptyList()),

                // PG_SNAPSHOT_ARRAY cases
                Arguments.of(PostgresDataType.PG_SNAPSHOTARRAY, "{123:456:789,987:654:321}",
                        Arrays.asList("123:456:789", "987:654:321")),
                Arguments.of(PostgresDataType.PG_SNAPSHOTARRAY, "{}", Collections.emptyList()),

                // TXID_SNAPSHOT_ARRAY cases
                Arguments.of(PostgresDataType.TXID_SNAPSHOTARRAY, "{123:456:789,987:654:321}",
                        Arrays.asList("123:456:789", "987:654:321")),
                Arguments.of(PostgresDataType.TXID_SNAPSHOTARRAY, "{}", Collections.emptyList())
        );
    }

    @BeforeEach
    void setUp() {
        handler = new SpecialTypeHandler();
    }

    @Test
    public void test_handle_xml() {
        String xmlValue = "<root><child>Test</child></root>";
        Object result = handler.process(PostgresDataType.XML, "xmlColumn", xmlValue);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(xmlValue));
    }

    @Test
    public void test_handle_pg_lsn() {
        String lsnValue = "16/B374D848";
        Object result = handler.process(PostgresDataType.PG_LSN, "lsnColumn", lsnValue);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(lsnValue));
    }

    @Test
    public void test_handle_uuid() {
        UUID uuidValue = UUID.randomUUID();
        Object result = handler.process(PostgresDataType.UUID, "uuidColumn", uuidValue.toString());
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(uuidValue.toString()));
    }

    @Test
    public void test_handle_tsvector() {
        String tsquery = "'a':1,6,10 'and':8 'ate':9 'cat':3 'fat':2,11 'mat':7 'on':5 'rat':12 'sat':4";
        Object result = handler.process(PostgresDataType.TSQUERY, "tsqueryColumn", tsquery);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(tsquery));
    }

    @Test
    public void test_handle_tsquery() {
        String tsquery = "'fat' & 'rat'";
        Object result = handler.process(PostgresDataType.TSQUERY, "tsqueryColumn", tsquery);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(tsquery));
    }

    @Test
    public void test_handle_pg_snapshot() {
        String snapshotValue = "123:456:789";
        Object result = handler.process(PostgresDataType.PG_SNAPSHOT, "pgSnapshotColumn", snapshotValue);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(snapshotValue));
    }

    @Test
    public void test_handle_txid_snapshot() {
        String txidSnapshotValue = "123:456:789";
        Object result = handler.process(PostgresDataType.TXID_SNAPSHOT, "txidSnapshotColumn", txidSnapshotValue);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(txidSnapshotValue));
    }

    @ParameterizedTest
    @MethodSource("provideMiscArrayData")
    public void test_handle_misc_array(PostgresDataType columnType, String value, List<String> expected) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);

        assertThat(result, is(instanceOf(List.class)));
        assertEquals(expected, result);
    }

    @Test
    public void test_handle_null_array() {
        assertNull(handler.process(PostgresDataType.XMLARRAY, "testColumn", null));
        assertNull(handler.process(PostgresDataType.PG_LSNARRAY, "testColumn", null));
        assertNull(handler.process(PostgresDataType.UUIDARRAY, "testColumn", null));
        assertNull(handler.process(PostgresDataType.TSVECTORARRAY, "testColumn", null));
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}