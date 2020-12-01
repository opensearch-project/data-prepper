package com.amazon.situp.plugins.source;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@SitupPlugin(name = "file", type = PluginType.SOURCE)
public class FileSource implements Source<Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
    private static final String ATTRIBUTE_PATH = "path";
    private static final String ATTRIBUTE_TIMEOUT = "write_timeout";
    private static final int WRITE_TIMEOUT = 5_000;

    private final String filePathToRead;
    private final int writeTimeout;
    private final String pipelineName;
    private boolean isStopRequested;


    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link FileSource} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public FileSource(final PluginSetting pluginSetting) {
        this((String) checkNotNull(pluginSetting, "PluginSetting cannot be null")
                        .getAttributeFromSettings(ATTRIBUTE_PATH),
                pluginSetting.getIntegerOrDefault(ATTRIBUTE_TIMEOUT, WRITE_TIMEOUT),
                pluginSetting.getPipelineName());
    }

    public FileSource(final String filePath, final int writeTimeout, final String pipelineName) {
        if (filePath == null || filePath.isEmpty()) {
            throw new RuntimeException(format("Pipeline [%s] - path is a required attribute for file source",
                    pipelineName));
        }
        this.filePathToRead = filePath;
        this.writeTimeout = writeTimeout;
        this.pipelineName = checkNotNull(pipelineName, "Pipeline name cannot be null");
        isStopRequested = false;
    }


    @Override
    public void start(final Buffer<Record<String>> buffer) {
        checkNotNull(buffer, format("Pipeline [%s] - buffer cannot be null for file source to start", pipelineName));
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePathToRead), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null && !isStopRequested) {
                buffer.write(new Record<>(line), writeTimeout);
            }
        } catch (IOException | TimeoutException ex) {
            LOG.error("Pipeline [{}] - Error processing the input file [{}]", pipelineName, filePathToRead, ex);
            throw new RuntimeException(format("Pipeline [%s] - Error processing the input file %s", pipelineName,
                    filePathToRead), ex);
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}