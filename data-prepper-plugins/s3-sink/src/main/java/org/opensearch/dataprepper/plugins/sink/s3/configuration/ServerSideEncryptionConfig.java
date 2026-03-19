/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class ServerSideEncryptionConfig {

    @JsonProperty("type")
    private ServerSideEncryptionType type = ServerSideEncryptionType.S3;

    @JsonProperty("kms_key_id")
    private String kmsKeyId;

    @JsonProperty("bucket_key_enabled")
    private Boolean bucketKeyEnabled = true;

    public ServerSideEncryptionType getType() {
        return type;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public Boolean getBucketKeyEnabled() {
        return bucketKeyEnabled;
    }

    public void applyTo(final PutObjectRequest.Builder builder) {
        builder.serverSideEncryption(type.getServerSideEncryption());

        if (type == ServerSideEncryptionType.KMS || type == ServerSideEncryptionType.KMS_DSSE) {
            if (kmsKeyId != null) {
                builder.ssekmsKeyId(kmsKeyId);
            }
            builder.bucketKeyEnabled(bucketKeyEnabled);
        }
    }
}
