/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.json;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

import java.util.List;

public class ParseJsonProcessorConfigTest {

    private ParseJsonProcessorConfig createObjectUnderTest() {
        return new ParseJsonProcessorConfig();
    }

    @Test
    public void test_when_defaultParseJsonProcessorConfig_then_returns_default_values() {
        final ParseJsonProcessorConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSource(), equalTo(ParseJsonProcessorConfig.DEFAULT_SOURCE));
        assertThat(objectUnderTest.getDestination(), equalTo(null));
        assertThat(objectUnderTest.getPointer(), equalTo(null));
        assertThat(objectUnderTest.getTagsOnFailure(), equalTo(null));
        assertThat(objectUnderTest.getOverwriteIfDestinationExists(), equalTo(true));
        assertThat(objectUnderTest.isDeleteSourceRequested(), equalTo(false));
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
            List<String> tagsList = List.of("tag1", "tag2");
            setField(ParseJsonProcessorConfig.class, config, "tagsOnFailure", tagsList);

            assertThat(config.getTagsOnFailure(), equalTo(tagsList));

            setField(ParseJsonProcessorConfig.class, config, "deleteSource", true);
            assertThat(config.isDeleteSourceRequested(), equalTo(true));
        }
    }
}
