package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.hamcrest.MatcherAssert.assertThat;


class TargetsParameterConfigTest {
    private TargetsParameterConfig targetsParameterConfig;
    private RegexParameterConfiguration regexParameterConfiguration;
    private TargetsParameterConfig createObjectUnderTest() {
        return new TargetsParameterConfig(null, null, null, null, null,null);
    }

    @BeforeEach
    void setup() throws NoSuchFieldException, IllegalAccessException{
        targetsParameterConfig = createObjectUnderTest();
        setField(TargetsParameterConfig.class, targetsParameterConfig, "target", "targetKey");
    }

    @Test
    void test_no_map_patterns_filepath_options_present(){
        assertFalse(targetsParameterConfig.hasMappings());
    }

    @Test
    void test_only_map_option_present() throws NoSuchFieldException, IllegalAccessException{
        setField(TargetsParameterConfig.class, targetsParameterConfig, "map", Collections.singletonMap("key1", "val1"));
        assertTrue(targetsParameterConfig.hasMappings());
    }

    @Test
    void test_no_patterns_under_regex() throws NoSuchFieldException, IllegalAccessException{
        regexParameterConfiguration = new RegexParameterConfiguration();
        setField(RegexParameterConfiguration.class, regexParameterConfiguration, "exact", true);
        setField(TargetsParameterConfig.class, targetsParameterConfig, "map", Collections.singletonMap("key1", "val1"));
        setField(TargetsParameterConfig.class, targetsParameterConfig, "regexParameterConfig", regexParameterConfiguration);
        assertFalse(targetsParameterConfig.isPatternPresent());
    }

    @Test
    void test_get_default() throws NoSuchFieldException, IllegalAccessException{
        assertNull(targetsParameterConfig.getDefaultValue());
        setField(TargetsParameterConfig.class, targetsParameterConfig, "defaultValue", "No match");
        assertThat(targetsParameterConfig.getDefaultValue(),is("No match"));
    }

    @Test
    void test_target_type_default(){
        assertThat(targetsParameterConfig.getTargetType(), is(TargetType.STRING));
    }

    @Test
    void test_get_target_type() throws NoSuchFieldException, IllegalAccessException{
        setField(TargetsParameterConfig.class, targetsParameterConfig, "targetType", TargetType.INTEGER);
        assertThat(targetsParameterConfig.getTargetType(), is(TargetType.INTEGER));
    }

}