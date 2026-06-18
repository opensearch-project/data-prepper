/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.formatdetection;

/**
 * Compression types detectable from magic bytes.
 */
public enum DetectedCompression {
    GZIP,
    ZSTD,
    SNAPPY,
    NONE
}
