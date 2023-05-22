package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalOutputFile implements OutputFile {

    private static final long DEFAULT_BLOCK_SIZE = 8L * 1024L;

    private final File file;

    public LocalOutputFile(File file) {
        this.file = file;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
            throw new IOException(
                    "Failed to create the file's directory at " + file.getParentFile().getAbsolutePath());
        }

        try {
            return new LocalFilePositionOutputStream(new RandomAccessFile(file, "rw"));
        } catch (FileNotFoundException e) {
            throw new IOException("Failed to create file: " + file.toString(), e);
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException("Failed to delete: " + file.toString());
            }
        }
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return true;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }

}