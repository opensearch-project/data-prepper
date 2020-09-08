package com.amazon.situp.plugins.source;

import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.source.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@SitupPlugin(name = "file", type = PluginType.SOURCE)
public class FileSource implements Source<Record<String>> {
    private static final String ATTRIBUTE_PATH = "path";
    private final String filePathToRead;
    private boolean isStopRequested;
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample.txt";

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link FileSource} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public FileSource(final PluginSetting pluginSetting) {
        this((String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_PATH));
    }

    public FileSource() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSource(final String filePath) {
        this.filePathToRead = filePath == null ? SAMPLE_FILE_PATH : filePath;
        isStopRequested = false;
    }


    @Override
    public void start(final Buffer<Record<String>> buffer) {
        checkNotNull(buffer, "buffer cannot be null for source to start");
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePathToRead), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null && !isStopRequested) {
                buffer.write(new Record<>(line));
            }
        } catch (IOException ex) {
            throw new RuntimeException(format("Error processing the input file %s", filePathToRead), ex);
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}