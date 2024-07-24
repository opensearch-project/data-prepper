package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator.DEFAULT_DEPRECATED_PREFIX;

class PluginConfigValueTranslatorTest {
    private static final String TEST_PREFIX = "testPrefix";
    private final TestPluginConfigValueTranslator objectUnderTest = new TestPluginConfigValueTranslator();

    @Test
    void testGetPrefix() {
        assertThat(objectUnderTest.getPrefix(), equalTo(TEST_PREFIX));
    }

    @Test
    void testGetDeprecatedPrefix() {
        assertThat(objectUnderTest.getDeprecatedPrefix(), equalTo(DEFAULT_DEPRECATED_PREFIX));
    }

    static class TestPluginConfigValueTranslator implements PluginConfigValueTranslator{

        @Override
        public Object translate(String value) {
            return null;
        }

        @Override
        public String getPrefix() {
            return TEST_PREFIX;
        }
    }
}