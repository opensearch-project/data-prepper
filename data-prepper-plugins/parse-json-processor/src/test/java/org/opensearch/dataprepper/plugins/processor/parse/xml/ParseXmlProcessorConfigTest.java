package org.opensearch.dataprepper.plugins.processor.parse.xml;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

public class ParseXmlProcessorConfigTest {

    private ParseXmlProcessorConfig createObjectUnderTest() {
        return new ParseXmlProcessorConfig();
    }

    @Test
    public void test_when_defaultParseXmlProcessorConfig_then_returns_default_values() {
        final ParseXmlProcessorConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSource(), equalTo(ParseXmlProcessorConfig.DEFAULT_SOURCE));
        assertThat(objectUnderTest.getDestination(), equalTo(null));
        assertThat(objectUnderTest.getPointer(), equalTo(null));
        assertThat(objectUnderTest.getTagsOnFailure(), equalTo(null));
        assertThat(objectUnderTest.getOverwriteIfDestinationExists(), equalTo(true));
    }

    @Nested
    class Validation {
        final ParseXmlProcessorConfig config = createObjectUnderTest();

        @Test
        void test_when_destinationIsWhiteSpaceOrFrontSlash_then_isValidDestinationFalse()
                throws NoSuchFieldException, IllegalAccessException {
            setField(ParseXmlProcessorConfig.class, config, "destination", "good destination");

            assertThat(config.isValidDestination(), equalTo(true));

            setField(ParseXmlProcessorConfig.class, config, "destination", "");

            assertThat(config.isValidDestination(), equalTo(false));

            setField(ParseXmlProcessorConfig.class, config, "destination", "    ");

            assertThat(config.isValidDestination(), equalTo(false));

            setField(ParseXmlProcessorConfig.class, config, "destination", "   /   ");

            assertThat(config.isValidDestination(), equalTo(false));
            List<String> tagsList = List.of("tag1", "tag2");
            setField(ParseXmlProcessorConfig.class, config, "tagsOnFailure", tagsList);

            assertThat(config.getTagsOnFailure(), equalTo(tagsList));
        }
    }
}
