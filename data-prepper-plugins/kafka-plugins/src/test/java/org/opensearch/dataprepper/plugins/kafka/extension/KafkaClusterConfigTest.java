/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.pipeline.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.MskBrokerConnectionType;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaClusterConfigTest {
    private static SimpleModule simpleModule = new SimpleModule()
            .addDeserializer(Duration.class, new DataPrepperDurationDeserializer())
            .addDeserializer(ByteCount.class, new ByteCountDeserializer());
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(simpleModule);

    private KafkaClusterConfig makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        final DataPrepperConfiguration dataPrepperConfiguration = OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
        assertThat(dataPrepperConfiguration, notNullValue());
        assertThat(dataPrepperConfiguration.getPipelineExtensions(), notNullValue());
        final Map<String, Object> kafkaClusterConfigMap = (Map<String, Object>) dataPrepperConfiguration.getPipelineExtensions().getExtensionMap().get("kafka_cluster_config");
        String json = OBJECT_MAPPER.writeValueAsString(kafkaClusterConfigMap);
        Reader reader = new StringReader(json);
        return OBJECT_MAPPER.readValue(reader, KafkaClusterConfig.class);
    }

    @Test
    void testConfigWithTestExtension() throws IOException {
        final KafkaClusterConfig kafkaClusterConfig = makeConfig(
                "src/test/resources/valid-data-prepper-config-with-kafka-cluster-extension.yaml");
        assertThat(kafkaClusterConfig.getBootStrapServers(), equalTo(List.of("localhost:9092")));
        assertThat(kafkaClusterConfig.getEncryptionConfig().getType(), equalTo(EncryptionType.NONE));
        assertThat(kafkaClusterConfig.getAwsConfig().getRegion(), equalTo("us-east-1"));
        assertThat(kafkaClusterConfig.getAwsConfig().getAwsMskConfig().getArn(), equalTo("test-arn"));
        assertThat(kafkaClusterConfig.getAwsConfig().getAwsMskConfig().getBrokerConnectionType(), equalTo(MskBrokerConnectionType.PUBLIC));
        assertThat(kafkaClusterConfig.getAuthConfig().getSaslAuthConfig().getAwsIamAuthConfig(), equalTo(AwsIamAuthConfig.DEFAULT));
    }
}
