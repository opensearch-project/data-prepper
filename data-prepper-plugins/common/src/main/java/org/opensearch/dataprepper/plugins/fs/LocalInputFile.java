/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.SeekableInputStream;
import org.opensearch.dataprepper.model.io.InputFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalInputFile implements InputFile {
    /**
     * Local file object.
     */
    private final File file;

    /**
     * Constructor.
     *
     * @param file the input file
     * @throws FileNotFoundException when file cannot be found.
     */
    public LocalInputFile(final File file)  {
        this.file = file;
    }

    @Override
    public long getLength()  {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new LocalInputStream(new RandomAccessFile(file, "r"));
    }
}
