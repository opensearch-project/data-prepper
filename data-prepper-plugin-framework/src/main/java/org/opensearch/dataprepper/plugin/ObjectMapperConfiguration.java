/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.pipeline.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.pipeline.parser.EnumDeserializer;
import org.opensearch.dataprepper.pipeline.parser.EventKeyDeserializer;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.time.Duration;
import java.util.Set;

/**
 * Application context for internal plugin framework beans.
 */
@Named
public class ObjectMapperConfiguration {
    static final Set<Class> TRANSLATE_VALUE_SUPPORTED_JAVA_TYPES = Set.of(
            String.class, Number.class, Long.class, Short.class, Integer.class, Double.class, Float.class,
            Boolean.class, Character.class, PluginConfigVariable.class);

    @Bean(name = "extensionPluginConfigObjectMapper")
    ObjectMapper extensionPluginConfigObjectMapper(
            final DataPrepperDeserializationProblemHandler dataPrepperDeserializationProblemHandler) {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        simpleModule.addDeserializer(Enum.class, new EnumDeserializer());

        simpleModule.addDeserializer(ByteCount.class, new ByteCountDeserializer());

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule)
                .addHandler(dataPrepperDeserializationProblemHandler);
    }

    @Bean(name = "pluginConfigObjectMapper")
    ObjectMapper pluginConfigObjectMapper(
            final VariableExpander variableExpander,
            final EventKeyFactory eventKeyFactory,
            final DataPrepperDeserializationProblemHandler dataPrepperDeserializationProblemHandler) {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        simpleModule.addDeserializer(ByteCount.class, new ByteCountDeserializer());
        simpleModule.addDeserializer(Enum.class, new EnumDeserializer());
        simpleModule.addDeserializer(EventKey.class, new EventKeyDeserializer(eventKeyFactory));
        TRANSLATE_VALUE_SUPPORTED_JAVA_TYPES.stream().forEach(clazz -> simpleModule.addDeserializer(
                clazz, new DataPrepperScalarTypeDeserializer<>(variableExpander, clazz)));

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule)
                .addHandler(dataPrepperDeserializationProblemHandler);
    }
}
