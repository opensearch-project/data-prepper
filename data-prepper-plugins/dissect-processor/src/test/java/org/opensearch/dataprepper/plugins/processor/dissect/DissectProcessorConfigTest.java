package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
        Map<String, String> targetTypeMap = Map.of("field1", "integer");
        setField(DissectProcessorConfig.class, dissectProcessorConfig, "targetTypes", targetTypeMap);

        assertTrue(dissectProcessorConfig.isTargetTypeValid());
        final Map<String, TargetType> actualMap = dissectProcessorConfig.getTargetTypes();

        assertThat(actualMap.size(), equalTo(targetTypeMap.size()));
        assertThat(actualMap.containsKey("field1"), equalTo(true));
        assertThat(actualMap.get("field1"), equalTo(TargetType.INTEGER));
    }

}