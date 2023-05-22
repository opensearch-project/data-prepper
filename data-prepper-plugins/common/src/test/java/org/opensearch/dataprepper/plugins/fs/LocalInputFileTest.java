package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.fs.LocalInputStream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalInputFileTest {

    private static final int FILE_LENGTH = 100;

    private static File testDataFile;
    private LocalInputFile inputFile;

    @BeforeAll
    public static void setUpAll() throws IOException{
        testDataFile = File.createTempFile( "LocalInputFileTest-", "txt");
        writeA100TimesToFile(testDataFile);

    }

    @BeforeEach
    public void setup() throws IOException{
        inputFile = new LocalInputFile(testDataFile);
    }

    @Test
    public void getLength_returnsInputLength() throws IOException {
        assertEquals(FILE_LENGTH, inputFile.getLength());
    }

    @Test
    public void newStream_returnsNewLocalInputStream() throws IOException {
        SeekableInputStream seekableInputStream = inputFile.newStream();

        Assertions.assertEquals(LocalInputStream.class, seekableInputStream.getClass());
    }

    private static void writeA100TimesToFile(File file) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            for (int i = 0; i < FILE_LENGTH; i++) {
                writer.write("a");
            }
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }
}

