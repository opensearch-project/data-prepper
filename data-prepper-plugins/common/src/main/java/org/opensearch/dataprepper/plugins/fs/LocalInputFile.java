package org.opensearch.dataprepper.plugins.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Random;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class LocalInputFile implements InputFile {
    /**
     * Local file object.
     */
    private final File file;

    /**
     * Constructor.
     *
     * @param path the input file path.
     * @throws FileNotFoundException when file cannot be found.
     */
    public LocalInputFile(final File file) throws FileNotFoundException {
        this.file = file;
    }

    @Override
    public long getLength() throws IOException {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new LocalInputStream(new RandomAccessFile(file, "r"));
    }
}
