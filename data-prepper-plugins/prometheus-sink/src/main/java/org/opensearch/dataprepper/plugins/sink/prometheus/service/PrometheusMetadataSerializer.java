/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes PrometheusMetricMetadata to protobuf format and injects it into WriteRequest.
 * <p>
 * Since the prometheus-remote-protocol library v1.0.1 doesn't include metadata support,
 * we manually serialize metadata using protobuf wire format and append it to the WriteRequest bytes.
 */
public class PrometheusMetadataSerializer {

    // Protobuf wire types
    private static final int WIRE_TYPE_VARINT = 0;
    private static final int WIRE_TYPE_LENGTH_DELIMITED = 2;

    // Field numbers from Prometheus prompb/types.proto and remote.proto
    private static final int WRITEREQUEST_METADATA_FIELD = 3;
    private static final int METADATA_TYPE_FIELD = 1;
    private static final int METADATA_METRIC_FAMILY_NAME_FIELD = 2;
    private static final int METADATA_HELP_FIELD = 4;
    private static final int METADATA_UNIT_FIELD = 5;

    /**
     * Injects metadata into an existing WriteRequest protobuf byte array.
     *
     * @param writeRequestBytes The original WriteRequest bytes
     * @param metadataList      List of metadata to inject
     * @return New byte array with metadata included
     * @throws IOException if serialization fails
     */
    public static byte[] injectMetadata(byte[] writeRequestBytes, List<PrometheusMetricMetadata> metadataList) throws IOException {
        if (metadataList == null || metadataList.isEmpty()) {
            return writeRequestBytes;
        }

        // Deduplicate metadata by metric family name - only keep one metadata per metric
        Map<String, PrometheusMetricMetadata> uniqueMetadata = new HashMap<>();
        for (PrometheusMetricMetadata metadata : metadataList) {
            uniqueMetadata.putIfAbsent(metadata.getMetricFamilyName(), metadata);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Write original WriteRequest bytes
        output.write(writeRequestBytes);

        // Append metadata fields
        for (PrometheusMetricMetadata metadata : uniqueMetadata.values()) {
            byte[] metadataBytes = serializeMetadata(metadata);
            // Write field tag for WriteRequest.metadata (field 3, length-delimited)
            writeTag(output, WRITEREQUEST_METADATA_FIELD, WIRE_TYPE_LENGTH_DELIMITED);
            writeVarInt(output, metadataBytes.length);
            output.write(metadataBytes);
        }

        return output.toByteArray();
    }

    /**
     * Serializes a single MetricMetadata message to protobuf bytes.
     */
    private static byte[] serializeMetadata(PrometheusMetricMetadata metadata) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Field 1: type (varint/enum)
        writeTag(output, METADATA_TYPE_FIELD, WIRE_TYPE_VARINT);
        writeVarInt(output, metadata.getType().getValue());

        // Field 2: metric_family_name (string)
        if (metadata.getMetricFamilyName() != null && !metadata.getMetricFamilyName().isEmpty()) {
            byte[] nameBytes = metadata.getMetricFamilyName().getBytes(StandardCharsets.UTF_8);
            writeTag(output, METADATA_METRIC_FAMILY_NAME_FIELD, WIRE_TYPE_LENGTH_DELIMITED);
            writeVarInt(output, nameBytes.length);
            output.write(nameBytes);
        }

        // Field 4: help (string)
        if (metadata.getHelp() != null && !metadata.getHelp().isEmpty()) {
            byte[] helpBytes = metadata.getHelp().getBytes(StandardCharsets.UTF_8);
            writeTag(output, METADATA_HELP_FIELD, WIRE_TYPE_LENGTH_DELIMITED);
            writeVarInt(output, helpBytes.length);
            output.write(helpBytes);
        }

        // Field 5: unit (string)
        if (metadata.getUnit() != null && !metadata.getUnit().isEmpty()) {
            byte[] unitBytes = metadata.getUnit().getBytes(StandardCharsets.UTF_8);
            writeTag(output, METADATA_UNIT_FIELD, WIRE_TYPE_LENGTH_DELIMITED);
            writeVarInt(output, unitBytes.length);
            output.write(unitBytes);
        }

        return output.toByteArray();
    }

    /**
     * Writes a protobuf field tag (field number + wire type).
     */
    private static void writeTag(ByteArrayOutputStream output, int fieldNumber, int wireType) {
        int tag = (fieldNumber << 3) | wireType;
        writeVarInt(output, tag);
    }

    /**
     * Writes a varint to the output stream.
     */
    private static void writeVarInt(ByteArrayOutputStream output, int value) {
        while (true) {
            if ((value & ~0x7F) == 0) {
                output.write(value);
                return;
            } else {
                output.write((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    /**
     * Estimates the total size of metadata when serialized.
     *
     * @param metadataList List of metadata
     * @return Estimated size in bytes
     */
    public static int estimateMetadataSize(List<PrometheusMetricMetadata> metadataList) {
        if (metadataList == null || metadataList.isEmpty()) {
            return 0;
        }

        // Deduplicate to match actual serialization
        Map<String, PrometheusMetricMetadata> uniqueMetadata = new HashMap<>();
        for (PrometheusMetricMetadata metadata : metadataList) {
            uniqueMetadata.putIfAbsent(metadata.getMetricFamilyName(), metadata);
        }

        int totalSize = 0;
        for (PrometheusMetricMetadata metadata : uniqueMetadata.values()) {
            // Add metadata message size
            int metadataSize = metadata.estimateSize();
            // Add WriteRequest field overhead (tag + length prefix)
            totalSize += 1 + computeVarIntSize(metadataSize) + metadataSize;
        }

        return totalSize;
    }

    private static int computeVarIntSize(int value) {
        if (value < 0) return 10;
        if (value < (1 << 7)) return 1;
        if (value < (1 << 14)) return 2;
        if (value < (1 << 21)) return 3;
        if (value < (1 << 28)) return 4;
        return 5;
    }
}
