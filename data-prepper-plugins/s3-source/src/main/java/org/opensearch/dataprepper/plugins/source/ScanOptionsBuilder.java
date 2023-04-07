/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.util.List;

public class ScanOptionsBuilder {
    private String startDate;
    private String range;
    private String bucket;
    private String expression;
    private S3SelectSerializationFormatOption serializationFormatOption;
    private List<String> keys;
    private Codec codec;
    private CompressionOption compressionOption;

    private CompressionType compressionType;

    private S3SelectCSVOption s3SelectCSVOption;

    private S3SelectJsonOption s3SelectJsonOption;

    private String expressionType;

    public String getExpressionType() {
        return expressionType;
    }

    public ScanOptionsBuilder setExpressionType(String expressionType) {
        this.expressionType = expressionType;
        return this;
    }

    public S3SelectCSVOption getS3SelectCSVOption() {
        return s3SelectCSVOption;
    }

    public ScanOptionsBuilder setS3SelectCSVOption(S3SelectCSVOption s3SelectCSVOption) {
        this.s3SelectCSVOption = s3SelectCSVOption;
        return this;
    }

    public S3SelectJsonOption getS3SelectJsonOption() {
        return s3SelectJsonOption;
    }

    public ScanOptionsBuilder setS3SelectJsonOption(S3SelectJsonOption s3SelectJsonOption) {
        this.s3SelectJsonOption = s3SelectJsonOption;
        return this;
    }

    public ScanOptionsBuilder setStartDate(String startDate) {
        this.startDate = startDate;
        return this;
    }

    public ScanOptionsBuilder setRange(String range) {
        this.range = range;
        return this;
    }

    public ScanOptionsBuilder setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public ScanOptionsBuilder setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public ScanOptionsBuilder setSerializationFormatOption(S3SelectSerializationFormatOption serializationFormatOption) {
        this.serializationFormatOption = serializationFormatOption;
        return this;
    }

    public ScanOptionsBuilder setKeys(List<String> keys) {
        this.keys = keys;
        return this;
    }

    public ScanOptionsBuilder setCodec(Codec codec) {
        this.codec = codec;
        return this;
    }

    public ScanOptionsBuilder setCompressionOption(CompressionOption compressionOption) {
        this.compressionOption = compressionOption;
        return this;
    }

    public ScanOptionsBuilder setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getRange() {
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

    public List<String> getKeys() {
        return keys;
    }

    public Codec getCodec() {
        return codec;
    }

    public CompressionOption getCompressionOption() {
        return compressionOption;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }
}