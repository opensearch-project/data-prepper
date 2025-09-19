package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class MappingsHandlerTest {
    private MappingsHandler mappingsHandler;
    private List<MappingsParameterConfig> mappingConfigs;
    private List<MappingsParameterConfig> fileMappingConfigs;

    @BeforeEach
    void setUp() {
        mappingsHandler = new MappingsHandler();
        mappingConfigs = new ArrayList<>();
        fileMappingConfigs = new ArrayList<>();
    }

    @Test
    void getCombinedMappings_whenMappingConfigsIsNull_returnsFileMappingConfigs() throws Exception {
        fileMappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));
        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(null, fileMappingConfigs);
        assertThat(result, is(fileMappingConfigs));
    }

    @Test
    void getCombinedMappings_whenMappingConfigsIsEmpty_returnsFileMappingConfigs() throws Exception {
        fileMappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));
        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(new ArrayList<>(), fileMappingConfigs);
        assertThat(result, is(fileMappingConfigs));
    }

    @Test
    void getCombinedMappings_whenFileMappingConfigsIsNull_returnsMappingConfigs() throws Exception {
        mappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));
        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(mappingConfigs, null);
        assertThat(result, is(mappingConfigs));
    }

    @Test
    void getCombinedMappings_withDifferentSources_combinesAllMappings() throws Exception {
        List<MappingsParameterConfig> result;

        // Create both configs
        fileMappingConfigs = createMappingConfigs("source2", Arrays.asList("target3", "target4"));
        mappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));

        // Initialize both configs
        for (MappingsParameterConfig config : mappingConfigs) {
            config.parseMappings();
            if (!config.isSourcePresent() || !config.isSourceFieldValid()) {
                throw new IllegalStateException("Invalid mapping config - source not valid");
            }
            if (!config.isTargetsPresent()) {
                throw new IllegalStateException("Invalid mapping config - targets not present");
            }
        }

        for (MappingsParameterConfig config : fileMappingConfigs) {
            config.parseMappings();
            if (!config.isSourcePresent() || !config.isSourceFieldValid()) {
                throw new IllegalStateException("Invalid file mapping config - source not valid");
            }
            if (!config.isTargetsPresent()) {
                throw new IllegalStateException("Invalid file mapping config - targets not present");
            }
        }

        result = mappingsHandler.getCombinedMappings(mappingConfigs, fileMappingConfigs);
        assertThat(result.size(), is(2));
        assertThat(result.get(0).getSource(), is("source1"));
        assertThat(result.get(0).getTargetsParameterConfigs().size(), is(2));
        assertThat(result.get(1).getSource(), is("source2"));
        assertThat(result.get(1).getTargetsParameterConfigs().size(), is(2));
    }

    @Test
    void getCombinedMappings_withSameSource_combinesTargets() throws Exception {
        mappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));
        fileMappingConfigs = createMappingConfigs("source1", Arrays.asList("target3", "target4"));

        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(mappingConfigs, fileMappingConfigs);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).getSource(), is("source1"));
        assertThat(result.get(0).getTargetsParameterConfigs().size(), is(4));
    }

    @Test
    void getCombinedMappings_withDuplicateTargets_ignoresDuplicates() throws Exception {
        mappingConfigs = createMappingConfigs("source1", Arrays.asList("target1", "target2"));
        fileMappingConfigs = createMappingConfigs("source1", Arrays.asList("target2", "target3"));

        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(mappingConfigs, fileMappingConfigs);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).getSource(), is("source1"));
        assertThat(result.get(0).getTargetsParameterConfigs().size(), is(3));
    }

    @Test
    void getCombinedMappings_whenExceptionOccurs_returnsNull() throws Exception {
        MappingsParameterConfig invalidConfig = new MappingsParameterConfig();
        mappingConfigs.add(invalidConfig);
        fileMappingConfigs = createMappingConfigs("source1", List.of("target1"));

        List<MappingsParameterConfig> result = mappingsHandler.getCombinedMappings(mappingConfigs, fileMappingConfigs);

        assertThat(result, is(nullValue()));
    }

    private List<MappingsParameterConfig> createMappingConfigs(String source, List<String> targets) throws Exception {
        List<TargetsParameterConfig> targetConfigs = new ArrayList<>();
        for (String target : targets) {
            TargetsParameterConfig targetConfig = new TargetsParameterConfig(
                    createMapEntries(createMapping("key1", "value1")),
                    target, null, null, null, null);
            targetConfigs.add(targetConfig);
        }

        MappingsParameterConfig config = new MappingsParameterConfig();
        setField(MappingsParameterConfig.class, config, "source", source);
        config.setTargetsParameterConfigs(targetConfigs);
        // Initialize mappings
        config.parseMappings();
        List<MappingsParameterConfig> mappingsParameterConfigList = new ArrayList<>();
        mappingsParameterConfigList.add(config);
        return mappingsParameterConfigList;
    }

    private Map.Entry<String, String> createMapping(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private Map<String, Object> createMapEntries(Map.Entry<String, String>... mappings) {
        Map<String, Object> finalMap = new HashMap<>();
        for (Map.Entry<String, String> mapping : mappings) {
            finalMap.put(mapping.getKey(), mapping.getValue());
        }
        return finalMap;
    }
}