package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class DissectProcessorConfigTest {
    private DissectProcessorConfig dissectProcessorConfig;
    private DissectProcessorConfig createObjectUnderTest() {
        return new DissectProcessorConfig();
    }

    @BeforeEach
    void setup(){
        dissectProcessorConfig = createObjectUnderTest();
    }

    @Test
    void test_get_map() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> dissectMap = Map.of("key1", "%{field1}");
        setField(DissectProcessorConfig.class, dissectProcessorConfig, "map", dissectMap);
        assertThat(dissectProcessorConfig.getMap(), is(dissectMap));
    }

    @Test
    void test_get_dissect_when() throws NoSuchFieldException, IllegalAccessException {
        String dissectWhen = "/test!=null";
        setField(DissectProcessorConfig.class, dissectProcessorConfig, "dissectWhen", dissectWhen);
        assertThat(dissectProcessorConfig.getDissectWhen(), is(dissectWhen));
    }

    @Test
    void test_get_targets_types() throws NoSuchFieldException, IllegalAccessException {
        Map<String, TargetType> targetTypeMap = Map.of("field1", TargetType.INTEGER);
        setField(DissectProcessorConfig.class, dissectProcessorConfig, "targetTypes", targetTypeMap);
        assertThat(dissectProcessorConfig.getTargetTypes(), is(targetTypeMap));
    }

}