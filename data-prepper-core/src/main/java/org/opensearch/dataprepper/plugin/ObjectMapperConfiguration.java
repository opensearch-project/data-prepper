package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.time.Duration;

/**
 * Application context for internal plugin framework beans.
 */
@Named
public class ObjectMapperConfiguration {
    @Bean(name = "extensionPluginConfigObjectMapper")
    ObjectMapper extensionPluginConfigObjectMapper() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule);
    }

    @Bean(name = "pluginConfigObjectMapper")
    ObjectMapper pluginConfigObjectMapper() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule);
    }
}
