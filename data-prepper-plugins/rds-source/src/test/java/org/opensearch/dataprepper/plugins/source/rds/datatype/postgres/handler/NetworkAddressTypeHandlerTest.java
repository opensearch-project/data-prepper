package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NetworkAddressTypeHandlerTest {
    private NetworkAddressTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new NetworkAddressTypeHandler();
    }
    @ParameterizedTest
    @CsvSource({
            "INET, 192.168.0.1",
            "INET, 2001:db8::1234",
            "CIDR, 192.168.0.0/24",
            "CIDR, 2001:db8::/32",
            "MACADDR, 08:00:2b:01:02:03",
            "MACADDR8, 08:00:2b:01:02:03:04:05"
    })
    public void test_handle_network_address_type(PostgresDataType columnType, String value) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}
