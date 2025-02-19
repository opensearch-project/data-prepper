/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class LocalDirectoryEncryptedDataKeyWriter implements EncryptedDataKeyWriter {
    private static final String KEY_NAME_FORMAT = "%s.key";
    private final Path encryptionKeyDirectory;

    public LocalDirectoryEncryptedDataKeyWriter(final String encryptionKeyDirectory) {
        this.encryptionKeyDirectory = Path.of(encryptionKeyDirectory);
    }

    @Override
    public void writeEncryptedDataKey(final String encryptedDataKey) throws IOException {
        if (!Files.exists(encryptionKeyDirectory)) {
            Files.createDirectories(encryptionKeyDirectory);
        }

        Path keyFile = encryptionKeyDirectory.resolve(buildKey());
        Files.writeString(keyFile, encryptedDataKey, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private String buildKey() {
        Instant now = Instant.now();
        return String.format(KEY_NAME_FORMAT, DateTimeFormatter.ISO_INSTANT.format(now));
    }
}
