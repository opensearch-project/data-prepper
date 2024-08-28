package org.opensearch.dataprepper.plugins.kinesis.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.pipeline.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KinesisLeaseConfigTest {
    private static SimpleModule simpleModule = new SimpleModule()
            .addDeserializer(Duration.class, new DataPrepperDurationDeserializer())
            .addDeserializer(ByteCount.class, new ByteCountDeserializer());
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(simpleModule);

    private KinesisLeaseConfig makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        final DataPrepperConfiguration dataPrepperConfiguration = OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
        assertThat(dataPrepperConfiguration, notNullValue());
        assertThat(dataPrepperConfiguration.getPipelineExtensions(), notNullValue());
        final Map<String, Object> kinesisLeaseConfigMap =
                (Map<String, Object>) dataPrepperConfiguration.getPipelineExtensions().getExtensionMap().get("kinesis");
        String json = OBJECT_MAPPER.writeValueAsString(kinesisLeaseConfigMap);
        Reader reader = new StringReader(json);
        return OBJECT_MAPPER.readValue(reader, KinesisLeaseConfig.class);
    }


    @Test
    void testConfigWithTestExtension() throws IOException {
        final KinesisLeaseConfig kinesisLeaseConfig = makeConfig(
                "src/test/resources/simple_pipeline_with_extensions.yaml");

        assertNotNull(kinesisLeaseConfig.getLeaseCoordinationTable());
        assertEquals(kinesisLeaseConfig.getLeaseCoordinationTable().getTableName(), "kinesis-pipeline-kcl");
        assertEquals(kinesisLeaseConfig.getLeaseCoordinationTable().getRegion(), "us-east-1");
        assertEquals(kinesisLeaseConfig.getLeaseCoordinationTable().getAwsRegion(), Region.US_EAST_1);
    }

}
