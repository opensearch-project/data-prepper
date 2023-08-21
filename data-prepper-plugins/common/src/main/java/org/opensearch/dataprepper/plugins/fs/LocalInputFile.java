/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.SeekableInputStream;
import org.opensearch.dataprepper.model.io.InputFile;

import java.io.File;
import java.io.IOException;

public class LocalInputFile implements InputFile {
    /**
     * Local file object.
     */
    private final File file;

    /**
     * Constructor.
     *
     * @param file the input file
     */
    public LocalInputFile(final File file)  {
        this.file = file;
    }

    /**
     * Get the length of the file.
     *
     * @return length of file
     */
    @Override
    public long getLength()  {
        return file.length();
    }

    /**
     * Create a new SeekableInputStream on the file.
     *
     * @return a SeekableInputStream on the file
     * @throws IOException if the input stream cannot be created.
     */
    @Override
    public SeekableInputStream newStream() throws IOException {
        return LocalInputStream.create(file);
    }
}
