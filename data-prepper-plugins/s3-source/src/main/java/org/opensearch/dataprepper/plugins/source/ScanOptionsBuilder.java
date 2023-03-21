/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;

import java.util.List;

public class ScanOptionsBuilder {
    private String startDate;
    private String range;
    private String bucket;
    private String query;
    private S3SelectSerializationFormatOption serializationFormatOption;
    private List<String> keys;
    private Codec codec;
    private CompressionOption compressionOption;

    private CompressionType compressionType;

    private FileHeaderInfo csvHeaderInfo;

    public FileHeaderInfo getCsvHeaderInfo() {
        return csvHeaderInfo;
    }

    public ScanOptionsBuilder setCsvHeaderInfo(FileHeaderInfo csvHeaderInfo) {
        this.csvHeaderInfo = csvHeaderInfo;
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

    public ScanOptionsBuilder setQuery(String query) {
        this.query = query;
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

    public String getQuery() {
        return query;
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