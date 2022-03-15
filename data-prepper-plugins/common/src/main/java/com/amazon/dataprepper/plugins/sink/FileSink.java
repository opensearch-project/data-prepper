/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import static java.lang.String.format;

@DataPrepperPlugin(name = "file", pluginType = Sink.class)
public class FileSink implements Sink<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    public static final String FILE_PATH = "path";

    private final String outputFilePath;
    private boolean isStopRequested;

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link FileSink} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public FileSink(final PluginSetting pluginSetting) {
        this((String) pluginSetting.getAttributeFromSettings(FILE_PATH));
    }

    public FileSink() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSink(final String outputFile) {
        this.outputFilePath = outputFile == null ? SAMPLE_FILE_PATH : outputFile;
        isStopRequested = false;
    }

    @Override
    public void output(Collection<Record<Object>> records) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for (final Record<Object> record : records) {
                checkTypeAndWriteObject(record.getData(), writer);
            }
        } catch (IOException ex) {
            throw new RuntimeException(format("Encountered exception opening/creating file %s", outputFilePath), ex);
        }
    }

    // Temporary function to support both trace and log ingestion pipelines.
    // TODO: This function should be removed with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    private void checkTypeAndWriteObject(final Object object, final BufferedWriter writer) throws IOException {
        if (object instanceof Event) {
            writer.write(((Event) object).toJsonString());
            writer.newLine();
        } else {
            writer.write(object.toString());
            writer.newLine();
        }
    }

    @Override
    public void shutdown() {

    }
}
