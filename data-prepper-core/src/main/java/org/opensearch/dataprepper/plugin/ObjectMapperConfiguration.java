package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
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
            String.class, Number.class, Boolean.class, Duration.class, Enum.class, Character.class);

    @Bean(name = "extensionPluginConfigObjectMapper")
    ObjectMapper extensionPluginConfigObjectMapper() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule);
    }

    @Bean(name = "pluginConfigObjectMapper")
    ObjectMapper pluginConfigObjectMapper(final VariableExpander variableExpander) {
        final SimpleModule simpleModule = new SimpleModule();
        TRANSLATE_VALUE_SUPPORTED_JAVA_TYPES.stream().forEach(clazz -> simpleModule.addDeserializer(
                clazz, new DataPrepperScalarTypeDeserializer<>(variableExpander, clazz)));

        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule);
    }
}
