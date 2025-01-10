/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriterConfig;

public class DlqConfiguration {
    @Getter
    @Valid
    @JsonProperty("s3")
    private S3DlqWriterConfig s3DlqWriterConfig;
}
