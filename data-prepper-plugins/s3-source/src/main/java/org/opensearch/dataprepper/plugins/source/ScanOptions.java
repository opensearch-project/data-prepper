/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
/**
 * Class consists the scan related properties.
 */
public class ScanOptions {
    private LocalDateTime startDateTime;
    private Duration range;
    private String bucket;
    private String expression;
    private S3SelectSerializationFormatOption serializationFormatOption;
    private CompressionOption compressionOption;

    private CompressionType compressionType;

    private List<String> includeKeyPaths;

    private List<String> excludeKeyPaths;

    private LocalDateTime endDateTime;

    public ScanOptions setStartDateTime(LocalDateTime startDateTime) {
        this.startDateTime = startDateTime;
        return this;
    }

    public ScanOptions setEndDateTime(LocalDateTime endDateTime) {
        this.endDateTime = endDateTime;
        return this;
    }

    public ScanOptions setRange(Duration range) {
        this.range = range;
        return this;
    }

    public ScanOptions setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public ScanOptions setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public ScanOptions setSerializationFormatOption(S3SelectSerializationFormatOption serializationFormatOption) {
        this.serializationFormatOption = serializationFormatOption;
        return this;
    }

    public ScanOptions setIncludeKeyPaths(List<String> includeKeyPaths) {
        this.includeKeyPaths = includeKeyPaths;
        return this;
    }

    public ScanOptions setExcludeKeyPaths(List<String> excludeKeyPaths) {
        this.excludeKeyPaths = excludeKeyPaths;
        return this;
    }

    public ScanOptions setCompressionOption(CompressionOption compressionOption) {
        this.compressionOption = compressionOption;
        return this;
    }

    public ScanOptions setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public LocalDateTime getStartDateTime() {
        return startDateTime;
    }

    public LocalDateTime getEndDateTime() {
        return endDateTime;
    }

    public Duration getRange() {
        return range;
    }

    public String getBucket() {
        return bucket;
    }

    public String getExpression() {
        return expression;
    }

    public S3SelectSerializationFormatOption getSerializationFormatOption() {
        return serializationFormatOption;
    }

    public CompressionOption getCompressionOption() {
        return compressionOption;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public List<String> getIncludeKeyPaths() {
        return includeKeyPaths;
    }

    public List<String> getExcludeKeyPaths() {
        return excludeKeyPaths;
    }
}