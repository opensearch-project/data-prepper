package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.core.Is.is;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import java.util.List;

class MappingsParameterConfigTest {

    private MappingsParameterConfig mappingsParameterConfig;

    @BeforeEach
    void setup() throws NoSuchFieldException, IllegalAccessException{
        mappingsParameterConfig = new MappingsParameterConfig();
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", "sourceKey");
    }

    @Test
    void test_get_source() {
        assertThat(mappingsParameterConfig.getSource(),is("sourceKey"));
    }

    @Test
    void test_source_present() {
        assertTrue(mappingsParameterConfig.isSourcePresent());
    }

    @Test
    void test_targets_present() {
        assertFalse(mappingsParameterConfig.isTargetsPresent());
    }

    @Test
    void test_source_field_valid_types() throws NoSuchFieldException, IllegalAccessException{
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", "key1");
        assertTrue(mappingsParameterConfig.isSourceFieldValid());
        assertThat(mappingsParameterConfig.getSource(), is("key1"));
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", List.of("key1", "key2", "key3"));
        assertTrue(mappingsParameterConfig.isSourceFieldValid());
        assertThat(mappingsParameterConfig.getSource(), is(List.of("key1", "key2", "key3")));
    }

    @Test
    void test_source_field_invalid_types() throws NoSuchFieldException, IllegalAccessException{
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", 200);
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", false);
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", 20.1);
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", List.of("key1", 200));
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
    }

    @Test
    void test_valid_source_array() throws NoSuchFieldException, IllegalAccessException {
        List<String> sourceList = List.of("sourceField1", "sourceField2");
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", sourceList);
        assertTrue(mappingsParameterConfig.isSourceFieldValid());
    }

    @Test
    void test_invalid_source_array_not_string_type() throws NoSuchFieldException, IllegalAccessException {
        List<Object> sourceList = List.of("sourceField1", 1);
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", sourceList);
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
    }

    @Test
    void test_valid_source_array_valid_common_path() throws NoSuchFieldException, IllegalAccessException {
        List<String> sourceList = List.of("field1/field2/sourceField1", "field1/field2/sourceField2");
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", sourceList);
        assertTrue(mappingsParameterConfig.isSourceFieldValid());
    }
    @Test
    void test_invalid_source_array_invalid_common_path() throws NoSuchFieldException, IllegalAccessException {
        List<String> sourceList = List.of("field1/field2/sourceField1", "field1/sourceField2");
        setField(MappingsParameterConfig.class, mappingsParameterConfig, "source", sourceList);
        assertFalse(mappingsParameterConfig.isSourceFieldValid());
    }

}