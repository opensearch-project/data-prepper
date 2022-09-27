/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.parsejson;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.amazon.dataprepper.plugins.processor.parsejson.ParseJsonProcessorConfig.DEFAULT_SOURCE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

public class ParseJsonProcessorConfigTest {

    private ParseJsonProcessorConfig createObjectUnderTest() {
        return new ParseJsonProcessorConfig();
    }

    @Test
    public void test_when_defaultParseJsonProcessorConfig_then_returns_default_values() {
        final ParseJsonProcessorConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSource(), equalTo(DEFAULT_SOURCE));
        assertThat(objectUnderTest.getDestination(), equalTo(null));
        assertThat(objectUnderTest.getPointer(), equalTo(null));
    }

    @Nested
    class Validation {
        final ParseJsonProcessorConfig config = createObjectUnderTest();

        @Test
        void test_when_destinationIsWhiteSpaceOrFrontSlash_then_isValidDestinationFalse()
                throws NoSuchFieldException, IllegalAccessException {
            setField(ParseJsonProcessorConfig.class, config, "destination", "good destination");

            assertThat(config.isValidDestination(), equalTo(true));

            setField(ParseJsonProcessorConfig.class, config, "destination", "");

            assertThat(config.isValidDestination(), equalTo(false));

            setField(ParseJsonProcessorConfig.class, config, "destination", "    ");

            assertThat(config.isValidDestination(), equalTo(false));

            setField(ParseJsonProcessorConfig.class, config, "destination", "   /   ");

            assertThat(config.isValidDestination(), equalTo(false));
        }
    }
}
