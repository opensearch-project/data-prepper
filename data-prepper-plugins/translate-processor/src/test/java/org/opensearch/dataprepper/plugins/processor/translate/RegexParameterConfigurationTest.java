package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.hamcrest.CoreMatchers.is;

class RegexParameterConfigurationTest {

    private RegexParameterConfiguration regexParameterConfiguration;

    @BeforeEach
    void setup(){
        regexParameterConfiguration = createObjectUnderTest();
    }

    @Test
    void test_get_patterns() throws NoSuchFieldException, IllegalAccessException{
        final Map<String, String> patternMap = Collections.singletonMap("key1", "val1");
        setField(RegexParameterConfiguration.class, regexParameterConfiguration, "patterns", patternMap);
        assertThat(regexParameterConfiguration.getPatterns(), is(patternMap));
    }

    @Test
    void test_get_exact() throws NoSuchFieldException, IllegalAccessException{
        setField(RegexParameterConfiguration.class, regexParameterConfiguration, "exact", false);
        assertFalse(regexParameterConfiguration.getExact());
    }

    @Test
    void test_default_exact_option(){
        assertTrue(regexParameterConfiguration.getExact());
    }

    private RegexParameterConfiguration createObjectUnderTest() {
        return new RegexParameterConfiguration();
    }

}