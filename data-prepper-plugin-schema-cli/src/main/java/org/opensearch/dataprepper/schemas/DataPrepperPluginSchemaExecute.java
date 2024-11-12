package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.schemas.module.DataPrepperModules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataPrepperPluginSchemaExecute implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperPluginSchemaExecute.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    static final String DEFAULT_PLUGINS_CLASSPATH = "org.opensearch.dataprepper.plugins";

    @CommandLine.Option(names = {"--plugin_type"}, required = true)
    private String pluginTypeName;

    @CommandLine.Option(names = {"--plugin_names"})
    private String pluginNames;

    @CommandLine.Option(names = {"--primary_fields_override"})
    private String primaryFieldsOverrideFilePath;

    @CommandLine.Option(names = {"--site.url"}, defaultValue = "https://opensearch.org")
    private String siteUrl;
    @CommandLine.Option(names = {"--site.baseurl"}, defaultValue = "/docs/latest")
    private String siteBaseUrl;

    @CommandLine.Option(names = {"--output_folder"})
    private String folderPath;

    public static void main(String[] args) {
        final int exitCode = new CommandLine(new DataPrepperPluginSchemaExecute()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        final PluginProvider pluginProvider = new ClasspathPluginProvider();
        final PrimaryFieldsOverride primaryFieldsOverride;
        try {
            primaryFieldsOverride = primaryFieldsOverrideFilePath == null ? new PrimaryFieldsOverride() :
                     OBJECT_MAPPER.readValue(new File(primaryFieldsOverrideFilePath), PrimaryFieldsOverride.class);
        } catch (IOException e) {
            throw new RuntimeException("primary fields override filepath does not exist. ", e);
        }
        final PluginConfigsJsonSchemaConverter pluginConfigsJsonSchemaConverter = new PluginConfigsJsonSchemaConverter(
                pluginProvider, new JsonSchemaConverter(DataPrepperModules.dataPrepperModules(), pluginProvider),
                primaryFieldsOverride, siteUrl, siteBaseUrl);
        final Class<?> pluginType = pluginConfigsJsonSchemaConverter.pluginTypeNameToPluginType(pluginTypeName);
        final Map<String, String> pluginNameToJsonSchemaMap = pluginConfigsJsonSchemaConverter.convertPluginConfigsIntoJsonSchemas(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, pluginType);
        Map<String, String> filteredPluginNameToJsonSchemaMap;
        if (pluginNames == null) {
            filteredPluginNameToJsonSchemaMap = pluginNameToJsonSchemaMap;
        } else {
            final Set<String> pluginNamesSet = Set.of(pluginNames.split(","));
            filteredPluginNameToJsonSchemaMap = pluginNamesSet.stream()
                    .filter(name -> {
                        if (!pluginNameToJsonSchemaMap.containsKey(name)) {
                            LOG.error("plugin name: {} not found", name);
                            return false;
                        }
                        return true;
                    })
                    .collect(Collectors.toMap(
                            Function.identity(),
                            pluginNameToJsonSchemaMap::get
                    ));
        }

        if (folderPath == null) {
            writeCollectionToConsole(filteredPluginNameToJsonSchemaMap.values());
        } else {
            writeMapToFiles(filteredPluginNameToJsonSchemaMap, folderPath);
        }
    }

    private static void writeCollectionToConsole(final Collection<String> values) {
        values.forEach(System.out::println);
    }

    private static void writeMapToFiles(final Map<String, String> map, final String folderPath) {
        // Ensure the directory exists
        final Path directory = Paths.get(folderPath);
        if (!Files.exists(directory)) {
            try {
                Files.createDirectories(directory);
            } catch (IOException e) {
                System.err.println("Error creating directory: " + e.getMessage());
                return;
            }
        }

        // Iterate through the map and write each entry to a file
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String fileName = entry.getKey() + ".json";
            final Path filePath = directory.resolve(fileName);

            try {
                Files.write(filePath, entry.getValue().getBytes());
                System.out.println("Written file: " + filePath);
            } catch (IOException e) {
                System.err.println("Error writing file " + fileName + ": " + e.getMessage());
            }
        }
    }
}
