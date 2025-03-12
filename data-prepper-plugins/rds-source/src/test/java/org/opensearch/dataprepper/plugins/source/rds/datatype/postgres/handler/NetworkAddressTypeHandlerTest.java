package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NetworkAddressTypeHandlerTest {
    private NetworkAddressTypeHandler handler;

    private static Stream<Arguments> provideNetworkAddressArrayData() {
        return Stream.of(
                Arguments.of(PostgresDataType.INETARRAY, "{192.168.0.1,2001:db8::1234}",
                        Arrays.asList("192.168.0.1", "2001:db8::1234")),
                Arguments.of(PostgresDataType.CIDRARRAY, "{192.168.0.0/24,2001:db8::/32}",
                        Arrays.asList("192.168.0.0/24", "2001:db8::/32")),
                Arguments.of(PostgresDataType.MACADDRARRAY, "{08:00:2b:01:02:03,01:23:45:67:89:ab}",
                        Arrays.asList("08:00:2b:01:02:03", "01:23:45:67:89:ab")),
                Arguments.of(PostgresDataType.MACADDR8ARRAY, "{08:00:2b:01:02:03:04:05,01:23:45:67:89:ab:cd:ef}",
                        Arrays.asList("08:00:2b:01:02:03:04:05", "01:23:45:67:89:ab:cd:ef")),
                Arguments.of(PostgresDataType.INETARRAY, "{}", Collections.emptyList())
        );
    }

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

    @ParameterizedTest
    @MethodSource("provideNetworkAddressArrayData")
    void test_handle_network_address_array(PostgresDataType columnType, String value, List<String> expected) {
        Object result = handler.process(columnType, "testColumn", value);
        assertThat(result, is(instanceOf(List.class)));
        assertEquals(expected, result);
    }

    @Test
    void test_handle_network_address_array_with_null_elements() {
        String value = "{192.168.0.1,NULL,2001:db8::1234}";
        Object result = handler.process(PostgresDataType.INETARRAY, "testColumn", value);
        assertThat(result, is(instanceOf(List.class)));
        assertEquals(Arrays.asList("192.168.0.1", null, "2001:db8::1234"), result);
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}
