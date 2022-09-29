/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.TestBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StdInSourceTests {
    private static final String SOURCE_CONTENT = "THIS IS A TEST\nexit";
    private static final String READ_CONTENT = "THIS IS A TEST";
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final int TEST_WRITE_TIMEOUT = 100;

    private InputStream defaultInputStream;

    @Before
    public void setup() {
        defaultInputStream = System.in;
        final ByteArrayInputStream streamForTests = new ByteArrayInputStream(
                SOURCE_CONTENT.getBytes(StandardCharsets.UTF_8));
        System.setIn(streamForTests);
    }

    @After
    public void tearDown() {
        System.setIn(defaultInputStream);
    }

    @Test
    public void testStdInSourceCreationUsingParameters() {
        final StdInSource stdInSource = new StdInSource(TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(stdInSource, notNullValue());
    }

    @Test
    public void testStdInSourceCreationUsingPluginSetting() {
        final PluginSetting pluginSetting = new PluginSetting("stdin", null);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        final StdInSource stdInSource = new StdInSource(pluginSetting);
        assertThat(stdInSource, notNullValue());
    }

    @Test
    public void testStdInSourceCreationWithNullPipelineName() {
        try {
            new StdInSource(TEST_WRITE_TIMEOUT, null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("Pipeline name cannot be null")));
        }
    }

    @Test
    public void testStdInSourceCreationWithNullPluginSetting() {
        try {
            new StdInSource(null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PluginSetting cannot be null")));
        }
    }

    @Test
    public void testStdInSourceWithNullBuffer() {
        final StdInSource stdInSource = new StdInSource(TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        try {
            stdInSource.start(null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - buffer cannot be null for source to start",
                    TEST_PIPELINE_NAME))));
        }
    }

    @Test
    public void testStdInSourceSuccessfulWriteToBuffer() {
        final Queue<Record<Event>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        final StdInSource stdInSource = new StdInSource(TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(buffer.size(), is(equalTo(0)));
        stdInSource.start(buffer);
        assertThat(buffer.size(), is(equalTo(1)));
        final Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = buffer.read(TEST_WRITE_TIMEOUT);
        final Collection<Record<Event>> recordsFromBuffer = readResult.getKey();
        assertThat(recordsFromBuffer.size(), is(equalTo(1)));
        recordsFromBuffer.forEach(actualRecord -> assertThat(
                actualRecord.getData().get("message", String.class), is(equalTo(READ_CONTENT))));
    }

    @Test
    public void testStdInSourceWhenStopped() {
        final Queue<Record<Event>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        final StdInSource stdInSource = new StdInSource(TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(buffer.size(), is(equalTo(0)));
        stdInSource.stop();
        stdInSource.start(buffer);
        assertThat(buffer.size(), is(equalTo(0)));
    }

    @Test
    public void testStdInSourceWhenBufferTimesout() {
        final Queue<Record<Event>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1, true);
        final StdInSource stdInSource = new StdInSource(TEST_WRITE_TIMEOUT, TEST_PIPELINE_NAME);
        assertThat(buffer.size(), is(equalTo(0)));
        try {
            stdInSource.start(buffer);
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is(equalTo(format("Pipeline [%s] - Timed out writing to buffer", TEST_PIPELINE_NAME))));
        }
        assertThat(buffer.size(), is(equalTo(0)));
    }

}
