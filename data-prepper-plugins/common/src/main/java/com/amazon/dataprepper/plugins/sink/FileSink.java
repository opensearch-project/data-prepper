/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import static java.lang.String.format;

@DataPrepperPlugin(name = "file", type = PluginType.SINK)
public class FileSink implements Sink<Record<String>> {
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
    public void output(Collection<Record<String>> records) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for (final Record<String> record : records) {
                writer.write(record.getData());
                writer.newLine();
            }
        } catch (IOException ex) {
            throw new RuntimeException(format("Encountered exception opening/creating file %s", outputFilePath), ex);
        }
    }

    @Override
    public void shutdown() {

    }
}
