/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.time.Duration;
import java.util.function.BiConsumer;

public class S3ObjectRequest {
    private final Buffer<Record<Event>> buffer;
    private final int numberOfRecordsToAccumulate;
    private final Duration bufferTimeout;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final String expression;
    private final S3SelectSerializationFormatOption serializationFormatOption;
    private final S3AsyncClient s3AsyncClient;
    private final S3SelectResponseHandlerFactory s3SelectResponseHandlerFactory;
    private final CompressionEngine compressionEngine;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final InputCodec codec;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3Client s3Client;
    private final CompressionType compressionType;
    private final S3SelectCSVOption s3SelectCSVOption;
    private final S3SelectJsonOption s3SelectJsonOption;
    private final String expressionType;


    private S3ObjectRequest(Builder builder) {
        this.buffer = builder.buffer;
        this.numberOfRecordsToAccumulate =builder.numberOfRecordsToAccumulate;

        this.bufferTimeout = builder.bufferTimeout;
        this.s3ObjectPluginMetrics = builder.s3ObjectPluginMetrics;
        this.expression = builder.expression;
        this.serializationFormatOption = builder.serializationFormatOption;
        this.s3AsyncClient =builder.s3AsyncClient;
        this.s3SelectResponseHandlerFactory = builder.s3SelectResponseHandlerFactory;
        this.compressionEngine = builder.compressionEngine;
        this.bucketOwnerProvider = builder.bucketOwnerProvider;
        this.codec = builder.codec;
        this.eventConsumer = builder.eventConsumer;
        this.s3Client = builder.s3Client;
        this.compressionType = builder.compressionType;
        this.s3SelectCSVOption = builder.s3SelectCSVOption;
        this.s3SelectJsonOption = builder.s3SelectJsonOption;
        this.expressionType = builder.expressionType;
    }

    public Buffer<Record<Event>> getBuffer() {
        return buffer;
    }

    public int getNumberOfRecordsToAccumulate() {
        return numberOfRecordsToAccumulate;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public S3ObjectPluginMetrics getS3ObjectPluginMetrics() {
        return s3ObjectPluginMetrics;
    }

    public String getExpression() {
        return expression;
    }

    public S3SelectSerializationFormatOption getSerializationFormatOption() {
        return serializationFormatOption;
    }

    public S3AsyncClient getS3AsyncClient() {
        return s3AsyncClient;
    }

    public S3SelectResponseHandlerFactory getS3SelectResponseHandlerFactory() {
        return s3SelectResponseHandlerFactory;
    }

    public CompressionEngine getCompressionEngine() {
        return compressionEngine;
    }

    public BucketOwnerProvider getBucketOwnerProvider() {
        return bucketOwnerProvider;
    }

    public InputCodec getCodec() {
        return codec;
    }

    public BiConsumer<Event, S3ObjectReference> getEventConsumer() {
        return eventConsumer;
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public S3SelectCSVOption getS3SelectCSVOption() {
        return s3SelectCSVOption;
    }

    public S3SelectJsonOption getS3SelectJsonOption() {
        return s3SelectJsonOption;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public static class Builder {
        private final Buffer<Record<Event>> buffer;
        private final int numberOfRecordsToAccumulate;
        private final Duration bufferTimeout;
        private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
        private BucketOwnerProvider bucketOwnerProvider;
        private String expression;
        private S3SelectSerializationFormatOption serializationFormatOption;
        private S3AsyncClient s3AsyncClient;
        private S3SelectResponseHandlerFactory s3SelectResponseHandlerFactory;
        private CompressionEngine compressionEngine;
        private InputCodec codec;
        private BiConsumer<Event, S3ObjectReference> eventConsumer;
        private S3Client s3Client;
        private CompressionType compressionType;
        private S3SelectCSVOption s3SelectCSVOption;
        private S3SelectJsonOption s3SelectJsonOption;
        private String expressionType;

        public Builder(final Buffer<Record<Event>> buffer,
                       final int numberOfRecordsToAccumulate,
                       final Duration bufferTimeout,
                       final S3ObjectPluginMetrics s3ObjectPluginMetrics){
            this.buffer = buffer;
            this.numberOfRecordsToAccumulate=numberOfRecordsToAccumulate;
            this.bufferTimeout = bufferTimeout;
            this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
        }

        public Builder bucketOwnerProvider(BucketOwnerProvider bucketOwnerProvider) {
            this.bucketOwnerProvider = bucketOwnerProvider;
            return this;
        }

        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        public Builder serializationFormatOption(S3SelectSerializationFormatOption serializationFormatOption) {
            this.serializationFormatOption = serializationFormatOption;
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder s3SelectResponseHandlerFactory(S3SelectResponseHandlerFactory s3SelectResponseHandlerFactory) {
            this.s3SelectResponseHandlerFactory = s3SelectResponseHandlerFactory;
            return this;
        }

        public Builder compressionEngine(CompressionEngine compressionEngine) {
            this.compressionEngine = compressionEngine;
            return this;
        }

        public Builder codec(InputCodec codec) {
            this.codec = codec;
            return this;
        }

        public Builder eventConsumer(BiConsumer<Event, S3ObjectReference> eventConsumer) {
            this.eventConsumer = eventConsumer;
            return this;
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder compressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public S3ObjectRequest build() {
            return new S3ObjectRequest(this);
        }

        public Builder s3SelectCSVOption(S3SelectCSVOption s3SelectCSVOption) {
            this.s3SelectCSVOption = s3SelectCSVOption;
            return this;
        }

        public Builder s3SelectJsonOption(S3SelectJsonOption s3SelectJsonOption) {
            this.s3SelectJsonOption = s3SelectJsonOption;
            return this;
        }
        public Builder expressionType(String expressionType) {
            this.expressionType = expressionType;
            return this;
        }

    }
}