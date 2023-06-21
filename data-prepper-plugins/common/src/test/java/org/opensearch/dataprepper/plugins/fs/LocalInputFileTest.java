package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

        final String content = new String(seekableInputStream.readAllBytes(), StandardCharsets.UTF_8);
        assertThat(content, equalTo("a".repeat(100)));
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

