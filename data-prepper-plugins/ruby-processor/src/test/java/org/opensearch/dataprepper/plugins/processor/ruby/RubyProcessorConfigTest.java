package org.opensearch.dataprepper.plugins.processor.ruby;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.opensearch.dataprepper.plugins.processor.ruby.RubyProcessorConfig.DEFAULT_PARAMETERS;
import static org.opensearch.dataprepper.plugins.processor.ruby.RubyProcessorConfig.DEFAULT_TAGS_ON_FAILURE;


public class RubyProcessorConfigTest {
    private static String SAMPLE_PATH = "example/path";
    private static final Map<String, String> SAMPLE_MAP = Map.of();

    private RubyProcessorConfig createObjectUnderTest() {
        return new RubyProcessorConfig();
    }

    @Test
    public void defaultRubyProcessorConfig_then_returns_default_values() { // todo: test naming guidance.
        final RubyProcessorConfig objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getParams(), equalTo(DEFAULT_PARAMETERS));
        assertThat(objectUnderTest.getTagsOnFailure(), equalTo(DEFAULT_TAGS_ON_FAILURE));

    }

    @Test
    public void when_settingTagsOnFailure_then_setsTagsOnFailure()
            throws NoSuchFieldException, IllegalAccessException
    {
        List<String > tagsOnFailure = new ArrayList<>(List.of("tag1", "tag2"));
        final RubyProcessorConfig objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "tagsOnFailure", tagsOnFailure);

        assertThat(objectUnderTest.getTagsOnFailure(), equalTo(tagsOnFailure));
    }

    @Nested
    class Validation {
        final RubyProcessorConfig rubyProcessorConfig = createObjectUnderTest();

        @Test
        void isExactlyOneOfCodeAndPathSpecified_true_cases()
            throws NoSuchFieldException, IllegalAccessException
        {
            // code specified, not path
            reflectivelySetField(rubyProcessorConfig, "code", "sample code");

            assertThat(rubyProcessorConfig.isExactlyOneOfCodeAndPathSpecified(), equalTo(true));

            // path specified, not code

            // reset OUT
            reflectivelySetField(rubyProcessorConfig, "code", null);
            reflectivelySetField(rubyProcessorConfig, "path", SAMPLE_PATH);

            assertThat(rubyProcessorConfig.isExactlyOneOfCodeAndPathSpecified(), equalTo(true));
        }

        @Test
        void isExactlyOneOfCodeAndPathSpecified_false_cases()
            throws NoSuchFieldException, IllegalAccessException
        {
            // neither code nor path specified

            assertThat(rubyProcessorConfig.isExactlyOneOfCodeAndPathSpecified(), equalTo(false));

            // both code and path specified
            reflectivelySetField(rubyProcessorConfig, "code", "sample code");
            reflectivelySetField(rubyProcessorConfig, "path", SAMPLE_PATH);

            assertThat(rubyProcessorConfig.isExactlyOneOfCodeAndPathSpecified(), equalTo(false));
        }


        @Test
        void isInitOnlySpecifiedWithCode_true_cases()
                throws NoSuchFieldException, IllegalAccessException
        {
            // init is not specified and code is not specified
            assertThat(rubyProcessorConfig.isInitOnlySpecifiedWithCode(), equalTo(true));

            // init is not specified and code is specified
            reflectivelySetField(rubyProcessorConfig, "code", "sample code");

            assertThat(rubyProcessorConfig.isInitOnlySpecifiedWithCode(), equalTo(true));

            // init is specified and code is specified
            reflectivelySetField(rubyProcessorConfig, "initCode", "sample init");

            assertThat(rubyProcessorConfig.isInitOnlySpecifiedWithCode(), equalTo(true));
        }

        @Test
        void isInitOnlySpecifiedWithCode_false_cases()
                throws NoSuchFieldException, IllegalAccessException
        {
            // init is specified and code is not specified
            reflectivelySetField(rubyProcessorConfig, "initCode", "sample init");

            assertThat(rubyProcessorConfig.isInitOnlySpecifiedWithCode(), equalTo(false));
        }
    }

    private void reflectivelySetField(final RubyProcessorConfig rubyProcessorConfig, final String fieldName,
                                      final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = RubyProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(rubyProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }


}
