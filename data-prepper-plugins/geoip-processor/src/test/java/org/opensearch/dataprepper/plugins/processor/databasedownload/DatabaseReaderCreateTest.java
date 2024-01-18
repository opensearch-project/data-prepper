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

import java.nio.file.Path;

@ExtendWith(MockitoExtension.class)
class DatabaseReaderCreateTest {

    private final Path databasePath = Path.of("/tmp/");

    @Test
    void createLoaderTest_with_cache() {
        DatabaseReader.Builder builder = DatabaseReaderCreate.createLoader(databasePath, 4096);
        Assertions.assertNotNull(builder);
    }
}
