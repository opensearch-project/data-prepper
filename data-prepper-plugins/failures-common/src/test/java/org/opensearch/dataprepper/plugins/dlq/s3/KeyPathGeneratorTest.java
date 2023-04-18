package org.opensearch.dataprepper.plugins.dlq.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.dlq.s3.KeyPathGenerator.UTC_ZONE_ID;

public class KeyPathGeneratorTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"/hello/world", "foobar"})
    public void testGenericKeyPath(final String keyPathPrefix) {
        final KeyPathGenerator keyPathGenerator = new KeyPathGenerator(keyPathPrefix);
        final String keyPath = keyPathGenerator.generate();
        assertThat(keyPath, is(equalTo(keyPathPrefix)));
    }

    @Test
    public void testKeyPathWithDate() {
        final ZonedDateTime now = LocalDateTime.now().atZone(ZoneId.systemDefault()).withZoneSameInstant(UTC_ZONE_ID);
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("/YYYY/MM/dd");
        final String expectedResult = formatter.format(now);
        final String keyPathPrefix = "/%{YYYY}/%{MM}/%{dd}";
        final KeyPathGenerator keyPathGenerator = new KeyPathGenerator(keyPathPrefix);
        final String keyPath = keyPathGenerator.generate();
        assertThat(keyPath, is((equalTo(expectedResult))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/%{YYYY}/%{MM}/%{dd}/%{ss}", "%{%{MM}}", "%{*.?}"})
    public void testKeyPathWithInvalidPattern(final String keyPathPrefix) {
        assertThrows(IllegalArgumentException.class, () -> new KeyPathGenerator(keyPathPrefix));
    }
}
