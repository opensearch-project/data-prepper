package org.opensearch.dataprepper.compression;

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompressionOptionTest {
    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void fromOptionValueValid(final CompressionOption option) {
        assertThat(CompressionOption.fromOptionValue(option.name()), is(option));
    }

    @Test
    void fromOptionValueInValid() {
        assertThrows(IllegalArgumentException.class, () -> CompressionOption.fromOptionValue("unknown"));
    }
}