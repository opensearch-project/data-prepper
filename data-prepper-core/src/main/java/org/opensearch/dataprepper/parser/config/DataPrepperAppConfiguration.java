/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.pipeline.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

@Configuration
public class DataPrepperAppConfiguration {

    @Bean
    public DataPrepperConfiguration dataPrepperConfiguration(
            final FileStructurePathProvider fileStructurePathProvider,
            final ObjectMapper objectMapper
    ) {
        final String dataPrepperConfigFileLocation = fileStructurePathProvider.getDataPrepperConfigFileLocation();
        if (dataPrepperConfigFileLocation != null) {
            final File configurationFile = new File(dataPrepperConfigFileLocation);
            try {
                final SimpleModule simpleModule = new SimpleModule()
                        .addDeserializer(Duration.class, new DataPrepperDurationDeserializer())
                        .addDeserializer(ByteCount.class, new ByteCountDeserializer());
                objectMapper.registerModule(simpleModule);
                return objectMapper.readValue(configurationFile, DataPrepperConfiguration.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid DataPrepper configuration file.", e);
            }
        }
        else {
            return new DataPrepperConfiguration();
        }
    }

    @Bean
    public PluginModel authentication(final DataPrepperConfiguration dataPrepperConfiguration) {
        return dataPrepperConfiguration.getAuthentication();
    }
}
