package org.opensearch.dataprepper.plugins.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.opensearch.dataprepper.plugins.fs.LocalOutputFile;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalOutputFileTest {

    private File mockFile;
    private LocalOutputFile localOutputFile;

    @BeforeEach
    public void setup() {
        mockFile = Mockito.mock(File.class);
        localOutputFile = new LocalOutputFile(mockFile);
    }

    @Test
    public void create_fileExists_throwsIOException() throws IOException {
        when(mockFile.exists()).thenReturn(true);
        assertThrows(IOException.class, () -> localOutputFile.create(4096));
    }

    @Test
    public void create_directoryCreationFails_throwsIOException() throws IOException {
        File parentFile = mock(File.class);
        when(mockFile.exists()).thenReturn(false);
        when(mockFile.getParentFile()).thenReturn(parentFile);
        when(parentFile.isDirectory()).thenReturn(false);
        when(parentFile.mkdirs()).thenReturn(false);

        assertThrows(IOException.class, () -> localOutputFile.create(4096));
    }

    @Test
    public void create_fileCreationFails_throwsIOException() throws IOException {
        File parentFile = mock(File.class);
        when(mockFile.exists()).thenReturn(false);
        when(mockFile.getParentFile()).thenReturn(parentFile);
        when(parentFile.isDirectory()).thenReturn(true);
        when(mockFile.createNewFile()).thenReturn(false);

        assertThrows(IOException.class, () -> localOutputFile.create(4096));
    }

    @Test
    public void createOrOverwrite_fileExistsButCannotDelete_throwsIOException() throws IOException {
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.delete()).thenReturn(false);

        assertThrows(IOException.class, () -> localOutputFile.createOrOverwrite(4096));
    }

    @Test
    public void supportsBlockSize_returnsTrue() {
        assertTrue(localOutputFile.supportsBlockSize());
    }

    @Test
    public void defaultBlockSize_returnsCorrectValue() {
        assertEquals(8L * 1024L, localOutputFile.defaultBlockSize());
    }

    @Test
    public void createOrOverwrite_fileDoesNotExist_invokesCreate(@TempDir File tempDir) throws IOException {
        File realFile = new File(tempDir, "test.parquet");
        LocalOutputFile realLocalOutputFile = new LocalOutputFile(realFile);

        realLocalOutputFile.createOrOverwrite(4096);

        assertTrue(realFile.exists());
    }
}

