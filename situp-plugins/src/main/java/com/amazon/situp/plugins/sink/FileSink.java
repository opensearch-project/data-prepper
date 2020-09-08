package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import static java.lang.String.format;

@SitupPlugin(name = "file", type = PluginType.SINK)
public class FileSink implements Sink<Record<String>> {
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    private final String outputFilePath;
    private boolean isStopRequested;

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link FileSink} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public FileSink(final PluginSetting pluginSetting) {
        this((String) pluginSetting.getAttributeFromSettings("path"));
    }

    public FileSink() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSink(final String outputFile) {
        this.outputFilePath = outputFile == null ? SAMPLE_FILE_PATH : outputFile;
        isStopRequested = false;
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for (final Record<String> record : records) {
                writer.write(record.getData());
                writer.newLine();
            }
            return true;
        } catch (IOException ex) {
            throw new RuntimeException(format("Encountered exception opening/creating file %s", outputFilePath), ex);
        }
    }
}
