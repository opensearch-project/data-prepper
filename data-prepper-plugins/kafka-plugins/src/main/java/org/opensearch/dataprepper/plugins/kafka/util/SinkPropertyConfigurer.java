/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;

import java.util.Properties;

/**
 * * This is static property configurer for related information given in pipeline.yml
 */
public class SinkPropertyConfigurer {

    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String KEY_SERIALIZER = "key.serializer";

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private static final String REGISTRY_URL = "schema.registry.url";

    public static  Properties getProducerProperties(final KafkaSinkConfig kafkaSinkConfig) {
        Properties properties = new Properties();
        setCommonServerProperties(properties,kafkaSinkConfig);
        setPropertiesForSerializer(properties, kafkaSinkConfig.getSerdeFormat(),kafkaSinkConfig);
        if (kafkaSinkConfig.getAuthConfig().getPlainTextAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setSaslPlainTextProperties(kafkaSinkConfig, properties);
        } else if (kafkaSinkConfig.getAuthConfig().getoAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setOauthProperties(kafkaSinkConfig, properties);
        }
        return properties;
    }

    private static void setCommonServerProperties(final Properties properties,final KafkaSinkConfig kafkaSinkConfig) {
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBootStrapServers());
        properties.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
    }

    private static void setPropertiesForSerializer(Properties properties, final String serdeFormat,final KafkaSinkConfig kafkaSinkConfig) {
        properties.put(KEY_SERIALIZER, StringSerializer.class.getName());
        validateForRegistryURL(kafkaSinkConfig);
        if (serdeFormat.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(VALUE_SERIALIZER, JsonSerializer.class.getName());
            properties.put(REGISTRY_URL, kafkaSinkConfig.getSchemaConfig().getRegistryURL());
        } else if (serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(VALUE_SERIALIZER, KafkaAvroSerializer.class.getName());
            properties.put(REGISTRY_URL, kafkaSinkConfig.getSchemaConfig().getRegistryURL());
        } else {
            properties.put(VALUE_SERIALIZER, StringSerializer.class.getName());
        }
    }

    private static void validateForRegistryURL(KafkaSinkConfig kafkaSinkConfig) {
       final String serdeFormat=kafkaSinkConfig.getSerdeFormat();
        if(serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())){
            if(kafkaSinkConfig.getSchemaConfig()==null ||kafkaSinkConfig.getSchemaConfig().getRegistryURL()==null||
                    kafkaSinkConfig.getSchemaConfig().getRegistryURL().isBlank()||kafkaSinkConfig.getSchemaConfig().getRegistryURL().isEmpty()){
                throw new RuntimeException("Schema registry is mandatory when serde type is avro");
            }
        }
        if(serdeFormat.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())){
            if(kafkaSinkConfig.getSchemaConfig()!=null &&
                    kafkaSinkConfig.getSchemaConfig().getRegistryURL()!=null){
                throw new RuntimeException("Schema registry is not required for type plaintext");
            }
        }
    }
}
