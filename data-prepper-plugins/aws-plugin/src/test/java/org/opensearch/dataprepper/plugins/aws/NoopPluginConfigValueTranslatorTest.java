package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class NoopPluginConfigValueTranslatorTest {
    private final NoopPluginConfigValueTranslator objectUnderTest = new NoopPluginConfigValueTranslator();

    @Test
    void testTranslate() {
        final String testInput = UUID.randomUUID().toString();
        assertThat(objectUnderTest.translate(testInput), equalTo(testInput));
    }
}