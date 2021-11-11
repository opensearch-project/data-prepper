package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

class DefaultPluginMapperTest {

    private DefaultPluginMapper defaultPluginMapper;

    @BeforeEach
    void createObjectUnderTest() {
        defaultPluginMapper = new DefaultPluginMapper();
    }

    @Test
    void mapPlugin_without_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.invalidMappingResourceNameData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                defaultPluginMapper.mapPlugin(logstashPlugin));

        String expectedMessage = "Unable to find mapping resource " + mappingResourceName;
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_with_incorrect_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.invalidMappingResourceData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                defaultPluginMapper.mapPlugin(logstashPlugin));

        String expectedMessage = "Unable to parse mapping file " + mappingResourceName;
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_without_plugin_name_in_mapping_file_throws_logstash_mapping_exception_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.noPluginNameMappingResourceData();
        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                defaultPluginMapper.mapPlugin(logstashPlugin));

        String expectedMessage = "The mapping file " + mappingResourceName + " has a null value for 'pluginName'.";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void mapPlugin_returns_plugin_model_Test() {
        LogstashPlugin logstashPlugin = TestDataProvider.pluginData();

        PluginModel actualPluginModel = defaultPluginMapper.mapPlugin(logstashPlugin);
        PluginModel expectedPluginModel = TestDataProvider.getSamplePluginModel();

        assertThat(expectedPluginModel.getPluginName(), equalTo(actualPluginModel.getPluginName()));
        assertThat(expectedPluginModel.getPluginSettings(), equalTo(actualPluginModel.getPluginSettings()));
    }

}