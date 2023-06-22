/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import com.maxmind.geoip2.DatabaseReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;

import java.io.IOException;
import java.nio.file.Path;

@ExtendWith(MockitoExtension.class)
class DatabaseReaderCreateTest {

    private final Path databasePath = Path.of("/tmp/");

    @Test
    void createLoaderTest_with_cache() throws IOException {
        DatabaseReader.Builder builder = DatabaseReaderCreate.createLoader(databasePath, LoadTypeOptions.CACHE, 4096);
        Assertions.assertNotNull(builder);
    }

    @Test
    void createLoaderTest_with_inMemory() throws IOException {
        DatabaseReader.Builder builder = DatabaseReaderCreate.createLoader(databasePath, LoadTypeOptions.INMEMORY, 4096);
        Assertions.assertNotNull(builder);
    }
}