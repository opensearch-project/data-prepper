/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.types;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteCountTest {
    private final Random random = new Random();

    @ParameterizedTest
    @ValueSource(strings = {
            ".1b",
            ".1kb",
            ".1mb",
            ".1gb",
            ".12b",
            ".0b",
            "b",
            "kb",
            "mb",
            "gb",
            "1 b",
            "1 kb",
            "1 mb",
            "1 gb",
            ".b",
            ".kb",
            ".mb",
            ".gb",
            "a",
            "badinput",
            "1b ",
            "1b trailing",
            "1kb ",
            "1kb trailing",
            "a1b",
            "1b!",
            "-0b",
            "-1b",
            "-1kb",
            "-1mb",
            "-1gb",
    })
    void parse_throws_exception_for_invalid_format(final String byteString) {
        final ByteCountParseException actualException = assertThrows(ByteCountParseException.class, () -> ByteCount.parse(byteString));

        assertThat(actualException.getMessage(), notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0",
            "1",
            "1024",
            "1.5",
    })
    void parse_throws_exception_for_missing_unit(final String byteString) {
        final ByteCountInvalidInputException actualException = assertThrows(ByteCountInvalidInputException.class, () -> ByteCount.parse(byteString));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString("Byte counts must have a unit"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0byte",
            "0bytes",
            "1bytes",
            "1nothing",
    })
    void parse_throws_exception_for_invalid_unit(final String byteString) {
        final ByteCountInvalidInputException actualException = assertThrows(ByteCountInvalidInputException.class, () -> ByteCount.parse(byteString));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString("Invalid byte unit"));
    }

    @ParameterizedTest
    @CsvSource({
            "0b, 0",
            "0kb, 0",
            "0mb, 0",
            "0gb, 0",
            "1b, 1",
            "8b, 8",
            "1024b, 1024",
            "2048b, 2048",
            "0.25kb, 256",
            "0.5kb, 512",
            "1kb, 1024",
            "2kb, 2048",
            "1.25kb, 1280",
            "1.5kb, 1536",
            "1024kb, 1048576",
            "2048kb, 2097152",
            "0.5mb, 524288",
            "1mb, 1048576",
            "2mb, 2097152",
            "5mb, 5242880",
            "1024mb, 1073741824",
            "0.5gb, 536870912",
            "1gb, 1073741824",
            "1.5gb, 1610612736",
            "2gb, 2147483648",
            "200gb, 214748364800"
    })
    void parse_returns_expected_byte_value(final String byteString, final long expectedBytes) {
        final ByteCount byteCount = ByteCount.parse(byteString);
        assertThat(byteCount, notNullValue());
        assertThat(byteCount.getBytes(), equalTo(expectedBytes));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0.1b",
            "0.5b",
            "1.1b",
            "1.9b",
            "20.1b"
    })
    void parse_throws_exception_for_explicit_fractional_bytes(final String byteString) {
        final ByteCountInvalidInputException actualException = assertThrows(ByteCountInvalidInputException.class, () -> ByteCount.parse(byteString));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString("fractional"));
    }

    @ParameterizedTest
    @CsvSource({
            "1.1kb, 1126",
            "1.1mb, 1153433",
            "0.49mb, 513802",
    })
    void parse_returns_rounded_bytes_for_implicit_fractional_bytes(final String byteString, final long expectedBytes) {
        final ByteCount byteCount = ByteCount.parse(byteString);
        assertThat(byteCount, notNullValue());
        assertThat(byteCount.getBytes(), equalTo(expectedBytes));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void ofBytes_returns_with_same_bytes(final long bytes) {
        final ByteCount byteCount = ByteCount.ofBytes(bytes);

        assertThat(byteCount, notNullValue());
        assertThat(byteCount.getBytes(), equalTo(bytes));
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, -2, -1024, Integer.MIN_VALUE, (long) Integer.MIN_VALUE - 100})
    void ofBytes_throws_with_invalid_bytes(final long bytes) {
        assertThrows(IllegalArgumentException.class, () -> ByteCount.ofBytes(bytes));
    }

    @Test
    void zeroBytes_returns_bytes_with_getBytes_equal_to_0() {
        assertThat(ByteCount.zeroBytes(), notNullValue());
        assertThat(ByteCount.zeroBytes().getBytes(), equalTo(0L));
    }

    @Test
    void zeroBytes_returns_same_instance() {
        assertThat(ByteCount.zeroBytes(), notNullValue());
        assertThat(ByteCount.zeroBytes(), sameInstance(ByteCount.zeroBytes()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 500, 512, 1000, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void hashCode_returns_same_value_for_same_bytes(final long bytes) {
        assertThat(ByteCount.ofBytes(bytes).hashCode(),
                equalTo(ByteCount.ofBytes(bytes).hashCode()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 500, 512, 1000, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void hashCode_returns_different_value_for_known_differences(final long bytes) {
        assertThat(ByteCount.ofBytes(bytes).hashCode(),
                not(equalTo(ByteCount.ofBytes(bytes+1).hashCode())));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 500, 512, 1000, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void equals_returns_true_for_same_value(final long bytes) {
        assertThat(ByteCount.ofBytes(bytes).equals(ByteCount.ofBytes(bytes)),
                equalTo(true));
    }

    @Test
    void equals_returns_false_for_null_other_object() {
        assertThat(ByteCount.ofBytes(1024).equals(null),
                equalTo(false));
    }

    @Test
    void equals_returns_false_for_other_type() {
        assertThat(ByteCount.ofBytes(1024).equals("1kb"),
                equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 500, 512, 1000, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void equals_returns_false_for_unequal_values(final long bytes) {
        assertThat(ByteCount.ofBytes(bytes).equals(ByteCount.ofBytes(bytes+1)),
                equalTo(false));
    }

    @ParameterizedTest
    @CsvSource({
            "0b, 0b",
            "0kb, 0b",
            "0mb, 0b",
            "0gb, 0b",
            "1b, 1b",
            "8b, 8b",
            "1024b, 1024b",
            "2048b, 2048b",
            "0.25kb, 256b",
            "0.5kb, 512b",
            "1kb, 1024b",
            "2kb, 2048b",
            "1.25kb, 1280b",
            "1.5kb, 1536b",
            "1024kb, 1048576b",
            "2048kb, 2097152b",
            "0.5mb, 524288b",
            "1mb, 1048576b",
            "2mb, 2097152b",
            "5mb, 5242880b",
            "1024mb, 1073741824b",
            "0.5gb, 536870912b",
            "1gb, 1073741824b",
            "1.5gb, 1610612736b",
            "2gb, 2147483648b",
            "200gb, 214748364800b"
    })
    void toString_returns_expected_byte_string(final String byteString, final String expectedString) {
        final ByteCount objectUnderTest = ByteCount.parse(byteString);
        assertThat(objectUnderTest.toString(), equalTo(expectedString));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 500, 512, 1000, 1024, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100})
    void toString_returns_string_that_parses_to_the_same_value(final long bytes) {
        final ByteCount objectUnderTest = ByteCount.ofBytes(bytes);
        final ByteCount parsedByteCount = ByteCount.parse(objectUnderTest.toString());
        assertThat(parsedByteCount, equalTo(objectUnderTest));
        assertThat(parsedByteCount.hashCode(), equalTo(objectUnderTest.hashCode()));
    }
}