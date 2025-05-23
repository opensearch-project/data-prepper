/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LocalDirectoryEncryptedDataKeySupplier implements EncryptedDataKeySupplier {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDirectoryEncryptedDataKeySupplier.class);

    private final Path encryptionKeyDirectory;
    private final AtomicReference<String> encryptedDataKey = new AtomicReference<>();

    public LocalDirectoryEncryptedDataKeySupplier(final String encryptionKeyDirectory) {
        this.encryptionKeyDirectory = Paths.get(encryptionKeyDirectory);
        encryptedDataKey.set(retrieveLatestFileContent());
    }

    @Override
    public String retrieveValue() {
        return encryptedDataKey.get();
    }

    @Override
    public void refresh() {
        encryptedDataKey.set(retrieveLatestFileContent());
    }

    private String retrieveLatestFileContent() {
        final Path latestFile = retrieveLatestFileKey();
        try {
            LOG.info("Reading latest key file: {}", latestFile);
            return Files.readString(latestFile, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            LOG.error("Error reading key file: {}", latestFile, ex);
            throw new RuntimeException(ex);
        }
    }

    private Path retrieveLatestFileKey() {
        try {
            List<Path> keyFiles = Files.list(encryptionKeyDirectory)
                    .filter(path -> path.toString().endsWith(".key"))
                    .sorted(Comparator.comparing(Path::getFileName).reversed())
                    .collect(Collectors.toList());

            if (keyFiles.isEmpty()) {
                throw new IllegalStateException("No data key files found in " + encryptionKeyDirectory);
            }
            return keyFiles.get(0);
        } catch (IOException ex) {
            LOG.error("Error accessing encryption key directory: {}", encryptionKeyDirectory, ex);
            throw new RuntimeException(ex);
        }
    }
}
