/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ServerSideEncryptionType {
    S3("s3", ServerSideEncryption.AES256),
    KMS("kms", ServerSideEncryption.AWS_KMS),
    KMS_DSSE("kms_dsse", ServerSideEncryption.AWS_KMS_DSSE);

    private static final Map<String, ServerSideEncryptionType> NAMES_MAP = Stream.of(values())
            .collect(Collectors.toMap(ServerSideEncryptionType::toString, v -> v));

    private final String name;
    private final ServerSideEncryption serverSideEncryption;

    ServerSideEncryptionType(final String name, final ServerSideEncryption serverSideEncryption) {
        this.name = name;
        this.serverSideEncryption = serverSideEncryption;
    }

    public ServerSideEncryption getServerSideEncryption() {
        return serverSideEncryption;
    }

    @Override
    public String toString() {
        return name;
    }

    @JsonCreator
    public static ServerSideEncryptionType fromString(final String value) {
        final ServerSideEncryptionType type = NAMES_MAP.get(value.toLowerCase());
        if (type == null) {
            throw new IllegalArgumentException("Invalid server_side_encryption type: " + value);
        }
        return type;
    }
}
