/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FileSourceConfig {
    static final String ATTRIBUTE_PATH = "path";
    static final String ATTRIBUTE_TYPE = "record_type";
    static final String ATTRIBUTE_FORMAT = "format";
    static final int DEFAULT_TIMEOUT = 5_000;
    static final String DEFAULT_TYPE = "string";
    static final String DEFAULT_FORMAT = "plain";
    static final String EVENT_TYPE = "event";

    @JsonProperty(ATTRIBUTE_PATH)
    private String filePathToRead;

    @JsonProperty("paths")
    private List<String> paths = Collections.emptyList();

    @JsonProperty("tail")
    private boolean tail = false;

    @JsonProperty(ATTRIBUTE_FORMAT)
    private String format = DEFAULT_FORMAT;

    @JsonProperty(ATTRIBUTE_TYPE)
    private String recordType = DEFAULT_TYPE;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("start_position")
    private StartPosition startPosition = StartPosition.END;

    @JsonProperty("poll_interval")
    private Duration pollInterval = Duration.ofSeconds(1);

    @JsonProperty("encoding")
    private String encoding = "UTF-8";

    @JsonProperty("read_buffer_size")
    private int readBufferSize = 65536;

    @JsonProperty("max_active_files")
    private int maxActiveFiles = 100;

    @JsonProperty("reader_threads")
    private int readerThreads = 2;

    @JsonProperty("max_read_time_per_file")
    private Duration maxReadTimePerFile = Duration.ofSeconds(5);

    @JsonProperty("rotate_wait")
    private Duration rotateWait = Duration.ofSeconds(5);

    @JsonProperty("rotation_drain_timeout")
    private Duration rotationDrainTimeout = Duration.ofSeconds(30);

    @JsonProperty("checkpoint_file")
    private String checkpointFile;

    @JsonProperty("checkpoint_interval")
    private Duration checkpointInterval = Duration.ofSeconds(15);

    @JsonProperty("checkpoint_cleanup_after")
    private Duration checkpointCleanupAfter = Duration.ofHours(24);

    @JsonProperty("fingerprint_bytes")
    private int fingerprintBytes = 1024;

    @JsonProperty("close_inactive")
    private Duration closeInactive = Duration.ofMinutes(5);

    @JsonProperty("close_removed")
    private boolean closeRemoved = true;

    @JsonProperty("batch_size")
    private int batchSize = 1000;

    @JsonProperty("batch_timeout")
    private Duration batchTimeout = Duration.ofSeconds(5);

    @JsonProperty("acknowledgment_timeout")
    private Duration acknowledgmentTimeout = Duration.ofSeconds(30);

    @JsonProperty("max_acknowledgment_retries")
    private int maxAcknowledgmentRetries = 3;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("include_file_metadata")
    private boolean includeFileMetadata = false;

    @JsonProperty("max_line_length")
    private int maxLineLength = 1048576;

    @JsonProperty("exclude_paths")
    private List<String> excludePaths = Collections.emptyList();

    public String getFilePathToRead() {
        return filePathToRead;
    }

    public List<String> getPaths() {
        return paths;
    }

    public boolean isTail() {
        return tail;
    }

    public List<String> getAllPaths() {
        final List<String> allPaths = new ArrayList<>(getPaths());
        if (filePathToRead != null && !allPaths.contains(filePathToRead)) {
            allPaths.add(filePathToRead);
        }
        return allPaths;
    }

    @JsonIgnore
    public FileFormat getFormat() {
        return FileFormat.getByName(format);
    }

    public String getRecordType() {
        return recordType;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public CompressionOption getCompression() {
        return compression;
    }

    public StartPosition getStartPosition() {
        return startPosition;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public String getEncoding() {
        return encoding;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public int getMaxActiveFiles() {
        return maxActiveFiles;
    }

    public int getReaderThreads() {
        return readerThreads;
    }

    public Duration getMaxReadTimePerFile() {
        return maxReadTimePerFile;
    }

    public Duration getRotateWait() {
        return rotateWait;
    }

    public Duration getRotationDrainTimeout() {
        return rotationDrainTimeout;
    }

    public String getCheckpointFile() {
        return checkpointFile;
    }

    public Duration getCheckpointInterval() {
        return checkpointInterval;
    }

    public Duration getCheckpointCleanupAfter() {
        return checkpointCleanupAfter;
    }

    public int getFingerprintBytes() {
        return fingerprintBytes;
    }

    public Duration getCloseInactive() {
        return closeInactive;
    }

    public boolean isCloseRemoved() {
        return closeRemoved;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getBatchTimeout() {
        return batchTimeout;
    }

    public Duration getAcknowledgmentTimeout() {
        return acknowledgmentTimeout;
    }

    public int getMaxAcknowledgmentRetries() {
        return maxAcknowledgmentRetries;
    }

    public boolean isAcknowledgments() {
        return acknowledgments;
    }

    public boolean isIncludeFileMetadata() {
        return includeFileMetadata;
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public List<String> getExcludePaths() {
        return excludePaths;
    }

    void validate() {
        if (tail) {
            Preconditions.checkArgument(
                    (filePathToRead != null && !filePathToRead.isEmpty()) || !paths.isEmpty(),
                    "At least one of path or paths is required when tail is enabled");
        } else {
            Objects.requireNonNull(filePathToRead, "File path is required");
        }
        Preconditions.checkArgument(recordType.equals(EVENT_TYPE) || recordType.equals(DEFAULT_TYPE), "Invalid type: must be either [event] or [string]");
        Preconditions.checkArgument(format.equals(DEFAULT_FORMAT) || format.equals("json"), "Invalid file format. Options are [json] and [plain]");
    }

    @AssertTrue(message = "The file source requires recordType to be event when using a codec.")
    boolean codeRequiresRecordTypeEvent() {
        return codec == null || recordType.equals(EVENT_TYPE);
    }
}
