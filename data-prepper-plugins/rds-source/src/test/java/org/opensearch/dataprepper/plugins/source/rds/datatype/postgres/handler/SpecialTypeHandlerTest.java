package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpecialTypeHandlerTest {
    private SpecialTypeHandler handler;

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
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}