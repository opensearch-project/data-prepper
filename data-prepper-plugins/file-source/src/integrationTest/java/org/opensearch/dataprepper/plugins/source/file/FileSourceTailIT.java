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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.event.TestEventFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FileSourceTailIT {

    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(15);
    private static final long ROTATION_DETECTION_DELAY_MS = 2000;
    private static final long SHORT_DELAY_MS = 1000;
    private static final long CLOSE_INACTIVE_WAIT_MS = 5000;
    private static final long DELETION_DETECTION_DELAY_MS = 3000;
    private static final long ACK_RETRY_INTERVAL_MS = 500;

    @TempDir
    Path tempDir;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private PluginMetrics pluginMetrics;
    private EventFactory eventFactory;
    private Buffer<Record<Object>> buffer;
    private List<Record<Object>> capturedRecords;
    private FileSource fileSource;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        pluginMetrics = PluginMetrics.fromNames("file", "test-pipeline");
        eventFactory = TestEventFactory.getTestEventFactory();
        capturedRecords = Collections.synchronizedList(new ArrayList<>());

        buffer = (Buffer<Record<Object>>) mock(Buffer.class);
        doAnswer(invocation -> {
            Record<Object> record = invocation.getArgument(0);
            capturedRecords.add(record);
            return null;
        }).when(buffer).write(any(Record.class), anyInt());
        doAnswer(invocation -> {
            Collection<Record<Object>> records = invocation.getArgument(0);
            capturedRecords.addAll(records);
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());
    }

    @AfterEach
    void tearDown() {
        if (fileSource != null) {
            fileSource.stop();
        }
    }

    @Test
    void tail_mode_reads_existing_lines_with_start_position_beginning() throws Exception {
        final Path logFile = tempDir.resolve("app.log");
        Files.write(logFile, List.of("line one", "line two", "line three"));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(3));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("line one"));
            assertThat(eventMessage(capturedRecords.get(1)), equalTo("line two"));
            assertThat(eventMessage(capturedRecords.get(2)), equalTo("line three"));
        });
    }

    @Test
    void tail_mode_with_start_position_end_skips_existing_content() throws Exception {
        final Path logFile = tempDir.resolve("existing.log");
        Files.write(logFile, List.of("old line 1", "old line 2"));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "end");

        fileSource = createSource(config);
        fileSource.start(buffer);

        Thread.sleep(ROTATION_DETECTION_DELAY_MS);
        assertThat(capturedRecords, hasSize(0));

        appendLine(logFile, "new line after start");

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("new line after start"));
        });
    }

    @Test
    void tail_mode_follows_appended_lines() throws Exception {
        final Path logFile = tempDir.resolve("append.log");
        Files.write(logFile, "initial\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        appendLine(logFile, "appended line 1");
        appendLine(logFile, "appended line 2");

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(3));
            assertThat(eventMessage(capturedRecords.get(1)), equalTo("appended line 1"));
            assertThat(eventMessage(capturedRecords.get(2)), equalTo("appended line 2"));
        });
    }

    @Test
    void tail_mode_discovers_files_via_glob_pattern() throws Exception {
        Files.write(tempDir.resolve("server1.log"), List.of("from server 1"));
        Files.write(tempDir.resolve("server2.log"), List.of("from server 2"));
        Files.write(tempDir.resolve("server.txt"), List.of("should not be read"));

        final String globPattern = tempDir.resolve("*.log").toString();
        final FileSourceConfig config = createTailConfig(
                null, List.of(globPattern), "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(2)));
    }

    @Test
    void tail_mode_excludes_paths_matching_exclude_pattern() throws Exception {
        Files.write(tempDir.resolve("app.log"), List.of("app log line"));
        Files.write(tempDir.resolve("debug.log"), List.of("debug log line"));

        final String globPattern = tempDir.resolve("*.log").toString();
        final String excludePattern = tempDir.resolve("debug*").toString();
        final FileSourceConfig config = createTailConfigWithExclude(
                globPattern, excludePattern, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("app log line"));
        });
    }

    @Test
    void tail_mode_detects_copytruncate_rotation() throws Exception {
        final Path logFile = tempDir.resolve("rotating.log");
        Files.write(logFile, "this is a long line before truncation happens here\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Files.write(logFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        Thread.sleep(ROTATION_DETECTION_DELAY_MS);
        appendLine(logFile, "short");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(greaterThanOrEqualTo(2)));
            assertThat(eventMessage(capturedRecords.get(capturedRecords.size() - 1)),
                    equalTo("short"));
        });
    }

    @Test
    void tail_mode_detects_create_rename_rotation() throws Exception {
        final Path logFile = tempDir.resolve("app.log");
        Files.write(logFile, "before rotation\n".getBytes(StandardCharsets.UTF_8));

        final String globPattern = tempDir.resolve("app.log").toString();
        final FileSourceConfig config = createTailConfig(
                globPattern, null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Files.move(logFile, tempDir.resolve("app.log.1"));
        Thread.sleep(ROTATION_DETECTION_DELAY_MS);
        Files.write(logFile, "after rotation\n".getBytes(StandardCharsets.UTF_8));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(greaterThanOrEqualTo(2)));
            assertThat(eventMessage(capturedRecords.get(capturedRecords.size() - 1)),
                    equalTo("after rotation"));
        });
    }

    @Test
    void tail_mode_includes_file_metadata() throws Exception {
        final Path logFile = tempDir.resolve("meta.log");
        Files.write(logFile, List.of("metadata test"));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            final Event event = (Event) capturedRecords.get(0).getData();
            assertThat(event.get("file_path", String.class),
                    equalTo(logFile.toAbsolutePath().toString()));
        });
    }

    @Test
    void tail_mode_resumes_from_checkpoint_after_restart() throws Exception {
        final Path logFile = tempDir.resolve("checkpoint.log");
        final Path checkpointFile = tempDir.resolve("checkpoint.json");
        Files.write(logFile, List.of("line 1", "line 2", "line 3"));

        final FileSourceConfig config = createTailConfigWithCheckpoint(
                logFile.toString(), "beginning", checkpointFile.toString());

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(3)));

        fileSource.stop();
        fileSource = null;
        Thread.sleep(SHORT_DELAY_MS);

        capturedRecords.clear();
        appendLine(logFile, "line 4");

        final FileSourceConfig config2 = createTailConfigWithCheckpoint(
                logFile.toString(), "beginning", checkpointFile.toString());
        fileSource = createSource(config2);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("line 4"));
        });
    }

    @Test
    void tail_mode_discovers_new_file_created_after_start() throws Exception {
        final String globPattern = tempDir.resolve("*.log").toString();
        final FileSourceConfig config = createTailConfig(
                null, List.of(globPattern), "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        Thread.sleep(SHORT_DELAY_MS);
        assertThat(capturedRecords, hasSize(0));

        Files.write(tempDir.resolve("new-file.log"), List.of("discovered after start"));

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("discovered after start"));
        });
    }

    @Test
    void tail_mode_handles_back_pressure_with_retry() throws Exception {
        final Path logFile = tempDir.resolve("backpressure.log");
        Files.write(logFile, "line1\nline2\nline3\n".getBytes(StandardCharsets.UTF_8));

        final AtomicInteger writeAttempts = new AtomicInteger(0);
        final int failFirstN = 3;

        @SuppressWarnings("unchecked")
        final Buffer<Record<Object>> slowBuffer = (Buffer<Record<Object>>) mock(Buffer.class);
        doAnswer(invocation -> {
            if (writeAttempts.incrementAndGet() <= failFirstN) {
                throw new TimeoutException("Buffer full");
            }
            Record<Object> record = invocation.getArgument(0);
            capturedRecords.add(record);
            return null;
        }).when(slowBuffer).write(any(Record.class), anyInt());

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(slowBuffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(3));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("line1"));
        });
        assertThat(writeAttempts.get(), greaterThanOrEqualTo(failFirstN + 3));
    }

    @Test
    void tail_mode_with_acknowledgements_commits_offset_on_positive_ack() throws Exception {
        final Path logFile = tempDir.resolve("ack.log");
        final Path checkpointFile = tempDir.resolve("ack-checkpoint.json");
        Files.write(logFile, "ack line 1\nack line 2\n".getBytes(StandardCharsets.UTF_8));

        final List<Consumer<Boolean>> ackCallbacks = Collections.synchronizedList(new ArrayList<>());

        final AcknowledgementSet mockAckSet = mock(AcknowledgementSet.class);
        final AcknowledgementSetManager ackManager = mock(AcknowledgementSetManager.class);
        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(0);
            ackCallbacks.add(callback);
            return mockAckSet;
        }).when(ackManager).create(any(), org.mockito.ArgumentMatchers.any(Duration.class));

        final FileSourceConfig config = buildConfigWithAcknowledgements(
                logFile.toString(), "beginning", checkpointFile.toString());

        fileSource = new FileSource(config, pluginMetrics, pluginFactory, eventFactory, ackManager);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(2)));

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(ackCallbacks, hasSize(greaterThanOrEqualTo(1))));

        ackCallbacks.get(0).accept(true);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(Files.exists(checkpointFile), equalTo(true));
            final String checkpointContent = Files.readString(checkpointFile);
            assertThat(checkpointContent.contains("committedOffset"), equalTo(true));
        });
    }

    @Test
    void tail_mode_with_acknowledgements_does_not_advance_offset_on_negative_ack() throws Exception {
        final Path logFile = tempDir.resolve("nack.log");
        final Path checkpointFile = tempDir.resolve("nack-checkpoint.json");
        Files.write(logFile, "nack line\n".getBytes(StandardCharsets.UTF_8));

        final List<Consumer<Boolean>> ackCallbacks = Collections.synchronizedList(new ArrayList<>());

        final AcknowledgementSet mockAckSet = mock(AcknowledgementSet.class);
        final AcknowledgementSetManager ackManager = mock(AcknowledgementSetManager.class);
        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(0);
            ackCallbacks.add(callback);
            return mockAckSet;
        }).when(ackManager).create(any(), org.mockito.ArgumentMatchers.any(Duration.class));

        final FileSourceConfig config = buildConfigWithAcknowledgements(
                logFile.toString(), "beginning", checkpointFile.toString());

        fileSource = new FileSource(config, pluginMetrics, pluginFactory, eventFactory, ackManager);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(ackCallbacks, hasSize(greaterThanOrEqualTo(1))));

        ackCallbacks.get(0).accept(false);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(Files.exists(checkpointFile), equalTo(true));
            final String checkpointContent = Files.readString(checkpointFile);
            assertThat(checkpointContent.contains("\"committedOffset\":0"), equalTo(true));
        });
    }

    @Test
    void tail_mode_max_active_files_queues_excess_files() throws Exception {
        for (int i = 0; i < 5; i++) {
            Files.write(tempDir.resolve("file" + i + ".log"),
                    ("content from file " + i + "\n").getBytes(StandardCharsets.UTF_8));
        }

        final String globPattern = tempDir.resolve("*.log").toString();
        final FileSourceConfig config = buildConfigWithMaxActiveFiles(
                globPattern, "beginning", 3);

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(greaterThanOrEqualTo(5))));
    }

    @Test
    void tail_mode_max_read_time_prevents_starvation() throws Exception {
        final Path largeFile = tempDir.resolve("large.log");
        final StringBuilder content = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            content.append("line ").append(i).append("\n");
        }
        Files.write(largeFile, content.toString().getBytes(StandardCharsets.UTF_8));

        final Path smallFile = tempDir.resolve("small.log");
        Files.write(smallFile, "small file line\n".getBytes(StandardCharsets.UTF_8));

        final String globPattern = tempDir.resolve("*.log").toString();
        final FileSourceConfig config = createTailConfig(
                null, List.of(globPattern), "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            final boolean hasSmallFileLine = capturedRecords.stream()
                    .anyMatch(r -> "small file line".equals(eventMessage(r)));
            assertThat(hasSmallFileLine, equalTo(true));
        });
    }

    @Test
    void tail_mode_close_inactive_releases_file_handle() throws Exception {
        final Path logFile = tempDir.resolve("inactive.log");
        Files.write(logFile, "initial line\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = buildConfigWithCloseInactive(
                logFile.toString(), "beginning", "PT2S");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Thread.sleep(CLOSE_INACTIVE_WAIT_MS);

        appendLine(logFile, "after inactive");

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(greaterThanOrEqualTo(2))));
    }

    @Test
    void tail_mode_handles_file_deleted_while_tailing() throws Exception {
        final Path logFile = tempDir.resolve("deleteme.log");
        Files.write(logFile, "will be deleted\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Files.delete(logFile);
        Thread.sleep(DELETION_DETECTION_DELAY_MS);

        assertThat(fileSource.areAcknowledgementsEnabled(), equalTo(false));
    }

    @Test
    void tail_mode_max_line_length_truncates_long_lines() throws Exception {
        final Path logFile = tempDir.resolve("longline.log");
        final String longLine = "x".repeat(5000);
        Files.write(logFile, (longLine + "\n").getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = buildConfigWithMaxLineLength(
                logFile.toString(), "beginning", 1024);

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            final String message = eventMessage(capturedRecords.get(0));
            assertThat(message.length(), equalTo(1024));
        });
    }

    @Test
    void tail_mode_back_pressure_during_rotation_does_not_lose_data() throws Exception {
        final Path logFile = tempDir.resolve("bp-rotate.log");
        Files.write(logFile, "line before rotation\n".getBytes(StandardCharsets.UTF_8));

        final AtomicInteger writeCount = new AtomicInteger(0);
        final int blockFirstNWrites = 2;

        @SuppressWarnings("unchecked")
        final Buffer<Record<Object>> slowBuffer = (Buffer<Record<Object>>) mock(Buffer.class);
        doAnswer(invocation -> {
            if (writeCount.incrementAndGet() <= blockFirstNWrites) {
                throw new TimeoutException("Buffer full - simulating back pressure");
            }
            Record<Object> record = invocation.getArgument(0);
            capturedRecords.add(record);
            return null;
        }).when(slowBuffer).write(any(Record.class), anyInt());

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(slowBuffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Files.move(logFile, tempDir.resolve("bp-rotate.log.1"));
        Thread.sleep(SHORT_DELAY_MS);
        Files.write(logFile, "line after rotation\n".getBytes(StandardCharsets.UTF_8));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(greaterThanOrEqualTo(2))));
    }

    @Test
    void tail_mode_negative_ack_retry_exhaustion_advances_offset() throws Exception {
        final Path logFile = tempDir.resolve("retry-exhaust.log");
        final Path checkpointFile = tempDir.resolve("retry-exhaust-checkpoint.json");
        Files.write(logFile, "retry line\n".getBytes(StandardCharsets.UTF_8));

        final List<Consumer<Boolean>> ackCallbacks = Collections.synchronizedList(new ArrayList<>());

        final AcknowledgementSet mockAckSet = mock(AcknowledgementSet.class);
        final AcknowledgementSetManager ackManager = mock(AcknowledgementSetManager.class);
        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(0);
            ackCallbacks.add(callback);
            return mockAckSet;
        }).when(ackManager).create(any(), org.mockito.ArgumentMatchers.any(Duration.class));

        final FileSourceConfig config = buildConfigWithAcknowledgementsAndRetries(
                logFile.toString(), "beginning", checkpointFile.toString(), 2);

        fileSource = new FileSource(config, pluginMetrics, pluginFactory, eventFactory, ackManager);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(ackCallbacks, hasSize(greaterThanOrEqualTo(1))));

        ackCallbacks.get(0).accept(false);
        Thread.sleep(ACK_RETRY_INTERVAL_MS);
        ackCallbacks.get(0).accept(false);
        Thread.sleep(ACK_RETRY_INTERVAL_MS);
        ackCallbacks.get(0).accept(false);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(Files.exists(checkpointFile), equalTo(true));
            final String content = Files.readString(checkpointFile);
            assertThat(content.contains("\"committedOffset\":0"), equalTo(false));
        });
    }

    @Test
    void tail_mode_checkpoint_is_persisted_periodically() throws Exception {
        final Path logFile = tempDir.resolve("checkpoint-persist.log");
        final Path checkpointFile = tempDir.resolve("persist-checkpoint.json");
        Files.write(logFile, "checkpoint test line\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfigWithCheckpoint(
                logFile.toString(), "beginning", checkpointFile.toString());

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(Files.exists(checkpointFile), equalTo(true));
            final String content = Files.readString(checkpointFile);
            assertThat(content.contains("readOffset"), equalTo(true));
        });
    }

    @Test
    void tail_mode_both_path_and_paths_are_merged() throws Exception {
        final Path singleFile = tempDir.resolve("single.log");
        Files.write(singleFile, "from single path\n".getBytes(StandardCharsets.UTF_8));

        final Path globFile = tempDir.resolve("glob-match.log");
        Files.write(globFile, "from glob path\n".getBytes(StandardCharsets.UTF_8));

        final String globPattern = tempDir.resolve("glob-*.log").toString();

        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", singleFile.toString());
        configMap.put("paths", List.of(globPattern));
        configMap.put("start_position", "beginning");
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("checkpoint_file", tempDir.resolve("merged-checkpoint.json").toString());
        final FileSourceConfig config = mapper.convertValue(configMap, FileSourceConfig.class);

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(2)));

        final List<String> messages = capturedRecords.stream()
                .map(this::eventMessage)
                .collect(Collectors.toList());
        assertThat(messages.contains("from single path"), equalTo(true));
        assertThat(messages.contains("from glob path"), equalTo(true));
    }

    @Test
    void tail_mode_close_removed_true_stops_reading_deleted_file() throws Exception {
        final Path logFile = tempDir.resolve("close-removed.log");
        Files.write(logFile, "will be removed\n".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(1)));

        Files.delete(logFile);
        Thread.sleep(DELETION_DETECTION_DELAY_MS);

        final int recordsAfterDelete = capturedRecords.size();
        Thread.sleep(ROTATION_DETECTION_DELAY_MS);
        assertThat(capturedRecords.size(), equalTo(recordsAfterDelete));
    }

    @Test
    void tail_mode_multiple_files_read_concurrently_with_reader_threads() throws Exception {
        for (int i = 0; i < 4; i++) {
            final Path file = tempDir.resolve("concurrent" + i + ".log");
            final StringBuilder content = new StringBuilder();
            for (int j = 0; j < 10; j++) {
                content.append("file").append(i).append("-line").append(j).append("\n");
            }
            Files.write(file, content.toString().getBytes(StandardCharsets.UTF_8));
        }

        final String globPattern = tempDir.resolve("concurrent*.log").toString();
        final FileSourceConfig config = createTailConfig(
                null, List.of(globPattern), "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(capturedRecords, hasSize(40)));
    }

    @Test
    void tail_mode_empty_file_produces_no_events() throws Exception {
        final Path emptyFile = tempDir.resolve("empty.log");
        Files.write(emptyFile, new byte[0]);

        final FileSourceConfig config = createTailConfig(
                emptyFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        Thread.sleep(DELETION_DETECTION_DELAY_MS);
        assertThat(capturedRecords, hasSize(0));

        appendLine(emptyFile, "content after empty");

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("content after empty"));
        });
    }

    @Test
    void tail_mode_partial_line_without_newline_flushed_on_read_timeout() throws Exception {
        final Path logFile = tempDir.resolve("partial.log");
        Files.write(logFile, "no newline at end".getBytes(StandardCharsets.UTF_8));

        final FileSourceConfig config = createTailConfig(
                logFile.toString(), null, "beginning");

        fileSource = createSource(config);
        fileSource.start(buffer);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(capturedRecords, hasSize(1));
            assertThat(eventMessage(capturedRecords.get(0)), equalTo("no newline at end"));
        });
    }

    private FileSource createSource(final FileSourceConfig config) {
        return new FileSource(config, pluginMetrics, pluginFactory, eventFactory, acknowledgementSetManager);
    }

    private FileSourceConfig createTailConfig(final String path, final List<String> paths, final String startPosition) {
        return buildConfig(path, paths, null, startPosition, null);
    }

    private FileSourceConfig createTailConfigWithExclude(final String globPattern, final String excludePattern,
                                                         final String startPosition) {
        return buildConfig(null, List.of(globPattern), List.of(excludePattern), startPosition, null);
    }

    private FileSourceConfig createTailConfigWithCheckpoint(final String path, final String startPosition,
                                                             final String checkpointFilePath) {
        return buildConfig(path, null, null, startPosition, checkpointFilePath);
    }

    private FileSourceConfig buildConfig(final String path, final List<String> paths,
                                          final List<String> excludePaths, final String startPosition,
                                          final String checkpointFile) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("start_position", startPosition != null ? startPosition : "beginning");
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        if (path != null) {
            configMap.put("path", path);
        }
        if (paths != null) {
            configMap.put("paths", paths);
        }
        if (excludePaths != null) {
            configMap.put("exclude_paths", excludePaths);
        }
        if (checkpointFile != null) {
            configMap.put("checkpoint_file", checkpointFile);
        } else {
            configMap.put("checkpoint_file", tempDir.resolve("default-checkpoint.json").toString());
        }
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithAcknowledgements(final String path, final String startPosition,
                                                              final String checkpointFile) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", path);
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("acknowledgments", true);
        configMap.put("batch_size", 10);
        configMap.put("checkpoint_file", checkpointFile);
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithMaxActiveFiles(final String globPattern, final String startPosition,
                                                            final int maxActiveFiles) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("paths", List.of(globPattern));
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("max_active_files", maxActiveFiles);
        configMap.put("include_file_metadata", true);
        configMap.put("checkpoint_file", tempDir.resolve("max-active-checkpoint.json").toString());
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithCloseInactive(final String path, final String startPosition,
                                                           final String closeInactive) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", path);
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("close_inactive", closeInactive);
        configMap.put("checkpoint_file", tempDir.resolve("close-inactive-checkpoint.json").toString());
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithMaxLineLength(final String path, final String startPosition,
                                                           final int maxLineLength) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", path);
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("max_line_length", maxLineLength);
        configMap.put("checkpoint_file", tempDir.resolve("maxline-checkpoint.json").toString());
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithAcknowledgementsAndRetries(final String path, final String startPosition,
                                                                        final String checkpointFile, final int maxRetries) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", path);
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("acknowledgments", true);
        configMap.put("batch_size", 10);
        configMap.put("max_acknowledgment_retries", maxRetries);
        configMap.put("checkpoint_file", checkpointFile);
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private FileSourceConfig buildConfigWithCheckpointCleanup(final String path, final String startPosition,
                                                               final String checkpointFile, final String cleanupAfter) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("tail", true);
        configMap.put("path", path);
        configMap.put("start_position", startPosition);
        configMap.put("reader_threads", 2);
        configMap.put("include_file_metadata", true);
        configMap.put("checkpoint_cleanup_after", cleanupAfter);
        configMap.put("checkpoint_file", checkpointFile);
        return mapper.convertValue(configMap, FileSourceConfig.class);
    }

    private void appendLine(final Path file, final String line) throws IOException {
        Files.write(file, (line + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    private String eventMessage(final Record<Object> record) {
        final Event event = (Event) record.getData();
        return event.get("message", String.class);
    }
}
