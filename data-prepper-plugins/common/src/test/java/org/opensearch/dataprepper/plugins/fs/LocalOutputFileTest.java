package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalOutputFileTest {

    private File testDataFile;
    private LocalOutputFile localOutputFile;

    @BeforeEach
    public void setup() throws IOException {
        testDataFile = File.createTempFile( "LocalOutputFileTest-", "txt");
        testDataFile.deleteOnExit();
        localOutputFile = new LocalOutputFile(testDataFile);
    }

    @Test
    public void create_fileCreationFails_throwsIOException() throws IOException {
        assertTrue(testDataFile.setReadOnly());

        assertThrows(IOException.class, () -> localOutputFile.create(8192L));
    }

    @Test
    public void createOrOverwrite_successful() throws IOException {
        final String inputString = "a".repeat(100);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.UTF_8);

        try (PositionOutputStream outputStream = localOutputFile.create(8192L)) {
            outputStream.write(inputBytes);
        }

        final String actualContent = Files.readString(testDataFile.toPath());

        assertEquals(inputString, actualContent);
    }

    @Test
    public void supportsBlockSize_returnsTrue() {
        assertTrue(localOutputFile.supportsBlockSize());
    }

    @Test
    public void defaultBlockSize_returnsCorrectValue() {
        assertEquals(8L * 1024L, localOutputFile.defaultBlockSize());
    }

}

