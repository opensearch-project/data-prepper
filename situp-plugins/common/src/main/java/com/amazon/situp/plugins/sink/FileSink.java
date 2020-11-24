package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import static java.lang.String.format;

@SitupPlugin(name = "file", type = PluginType.SINK)
public class FileSink implements Sink<Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
    private static final String ATTRIBUTE_PATH = "path";
    private static final String NULL_PATH = null;
    private final String outputFilePath;
    private final String pipelineName;
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
        this(pluginSetting.getStringOrDefault(ATTRIBUTE_PATH, NULL_PATH), pluginSetting.getPipelineName());
    }

    public FileSink(final String outputFile, final String pipelineName) {
        if (outputFile == null || outputFile.isEmpty()) {
            throw new RuntimeException(format("Pipeline [%s] - path is a required attribute for file sink",
                    pipelineName));
        }
        this.outputFilePath = outputFile;
        this.pipelineName = pipelineName;
        isStopRequested = false;
    }

    @Override
    public void output(Collection<Record<String>> records) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for (final Record<String> record : records) {
                writer.write(record.getData());
                writer.newLine();
            }
        } catch (IOException ex) {
            LOG.error("Pipeline [{}] - Encountered exception opening/creating [{}] for file sink",
                    pipelineName, outputFilePath, ex);
            throw new RuntimeException(format("Pipeline [%s] - Encountered exception opening/creating [%s] for " +
                    "file sink", pipelineName, outputFilePath), ex);
        }
    }
}
