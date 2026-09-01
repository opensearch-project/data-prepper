/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.formatdetection;

/**
 * Result of detecting the format and compression of a data sample.
 */
public class FormatDetectionResult {
    private final DetectedCompression compression;
    private final DetectedFormat format;
    private final Confidence confidence;

    public FormatDetectionResult(final DetectedCompression compression,
                                 final DetectedFormat format,
                                 final Confidence confidence) {
        this.compression = compression;
        this.format = format;
        this.confidence = confidence;
    }

    public DetectedCompression getCompression() {
        return compression;
    }

    public DetectedFormat getFormat() {
        return format;
    }

    public Confidence getConfidence() {
        return confidence;
    }

    @Override
    public String toString() {
        return "FormatDetectionResult{compression=" + compression +
                ", format=" + format +
                ", confidence=" + confidence + "}";
    }
}
