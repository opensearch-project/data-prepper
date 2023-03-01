package org.opensearch.dataprepper.plugins.sink.opensearch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LocalFileDLQWriter implements DLQWriter {
    private final BufferedWriter bufferedWriter;

    public LocalFileDLQWriter(final String dlqFile) throws IOException {
        bufferedWriter = Files.newBufferedWriter(
                Paths.get(dlqFile), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public void write(final String content) throws IOException {
        bufferedWriter.write(content);
    }

    @Override
    public void close() throws IOException {
        bufferedWriter.close();
    }
}
