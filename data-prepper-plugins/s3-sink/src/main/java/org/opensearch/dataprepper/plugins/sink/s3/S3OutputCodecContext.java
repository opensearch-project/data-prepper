/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

public class S3OutputCodecContext extends OutputCodecContext {
    private final CompressionOption compressionOption;

    public S3OutputCodecContext(OutputCodecContext outputCodecContext, CompressionOption compressionOption) {
        super(outputCodecContext.getTagsTargetKey(), outputCodecContext.getIncludeKeys(), outputCodecContext.getExcludeKeys());
        this.compressionOption = compressionOption;
    }

    public CompressionOption getCompressionOption() {
        return compressionOption;
    }
}
