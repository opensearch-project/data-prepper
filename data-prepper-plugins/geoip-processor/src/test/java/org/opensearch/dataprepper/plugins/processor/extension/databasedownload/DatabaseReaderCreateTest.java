/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatabaseReaderCreateTest {
    @Mock
    private Path path;

    @Test
    void createLoaderTest() throws IOException {
        final String testFileURL = "https://github.com/maxmind/MaxMind-DB/raw/main/test-data/GeoLite2-City-Test.mmdb";
        final File file = File.createTempFile( "GeoIP2-City-Test", ".mmdb");

        final BufferedInputStream in = new BufferedInputStream(new URL(testFileURL).openStream());
        FileUtils.copyInputStreamToFile(in, file);
        when(path.toFile()).thenReturn(file);

        DatabaseReader databaseReader = DatabaseReaderCreate.createLoader(path, 4096);
        Assertions.assertNotNull(databaseReader);
        in.close();
        file.deleteOnExit();
    }
}
