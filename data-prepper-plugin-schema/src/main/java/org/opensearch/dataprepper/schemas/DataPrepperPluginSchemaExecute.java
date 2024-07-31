package org.opensearch.dataprepper.schemas;

import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;

public class DataPrepperPluginSchemaExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperPluginSchemaExecute.class);
    static final String DEFAULT_PLUGINS_CLASSPATH = "org.opensearch.dataprepper.plugins";

    public static void main(String[] args) {
        final String pluginTypeName = System.getProperty("plugin_type");
        final String pluginNames = System.getProperty("plugin_names");
        final List<Module> modules = List.of(
                new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
        );
        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(DEFAULT_PLUGINS_CLASSPATH))
                .setScanners(Scanners.TypesAnnotated, Scanners.SubTypes));
        final PluginConfigsJsonSchemaConverter pluginConfigsJsonSchemaConverter = new PluginConfigsJsonSchemaConverter(
                reflections, new JsonSchemaConverter(modules));
        final Class<?> pluginType = pluginConfigsJsonSchemaConverter.pluginTypeNameToPluginType(pluginTypeName);
        final Map<String, String> pluginNameToJsonSchemaMap = pluginConfigsJsonSchemaConverter.convertPluginConfigsIntoJsonSchemas(
                        SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, pluginType);
        if (pluginNames == null) {
            System.out.println(pluginNameToJsonSchemaMap.values());
        } else {
            final Set<String> pluginNamesSet = Set.of(pluginNames.split(","));
            final List<String> result = pluginNamesSet.stream().flatMap(name -> {
                if (!pluginNameToJsonSchemaMap.containsKey(name)) {
                    LOG.error("plugin name: {} not found", name);
                    return Stream.empty();
                }
                return Stream.of(pluginNameToJsonSchemaMap.get(name));
            }).collect(Collectors.toList());
            System.out.println(result);
        }
    }
}
