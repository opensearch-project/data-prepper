/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FileSourceTests {
    private static final Logger LOG = LoggerFactory.getLogger(FileSourceTests.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };
    private static final String TEST_FILE_PATH_PLAIN = "src/test/resources/test-file-source-plain.tst";

    private FileSourceConfig fileSourceConfig;

    private Map<String, Object> pluginSettings;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @BeforeEach
    void setUp() {
        pluginSettings = new HashMap<>();

        pluginSettings.put(FileSourceConfig.ATTRIBUTE_TYPE, FileSourceConfig.EVENT_TYPE);
        pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, TEST_FILE_PATH_PLAIN);
    }

    private FileSource createObjectUnderTest() {
        fileSourceConfig = OBJECT_MAPPER.convertValue(pluginSettings, FileSourceConfig.class);
        return new FileSource(fileSourceConfig, pluginMetrics, pluginFactory, TestEventFactory.getTestEventFactory());
    }

    @Nested
    class WithRecord {
        private static final String TEST_PIPELINE_NAME = "pipeline";
        private static final String TEST_FILE_PATH_JSON = "src/test/resources/test-file-source-json.tst";
        private static final String TEST_FILE_PATH_INVALID_JSON = "src/test/resources/test-file-source-invalid-json.tst";
        private static final String FILE_DOES_NOT_EXIST = "file_does_not_exist";

        private FileSource fileSource;

        private Buffer<Record<Object>> buffer;

        private List<Record<Object>> expectedEventsPlain;
        private List<Record<Object>> expectedEventsJson;
        private List<Record<Object>> expectedEventsInvalidJson;


        @BeforeEach
        public void setup() {
            expectedEventsPlain = new ArrayList<>();
            expectedEventsJson = new ArrayList<>();
            expectedEventsInvalidJson = new ArrayList<>();

            // plain
            final String expectedPlainFirstLine = "THIS IS A PLAINTEXT LINE";
            final String expectedPlainSecondLine = "THIS IS ANOTHER PLAINTEXT LINE";

            final Record<Object> firstEventPlain = createRecordEventWithKeyValuePair(FileSource.MESSAGE_KEY, expectedPlainFirstLine);
            final Record<Object> secondEventPlain = createRecordEventWithKeyValuePair(FileSource.MESSAGE_KEY, expectedPlainSecondLine);

            expectedEventsPlain.add(firstEventPlain);
            expectedEventsPlain.add(secondEventPlain);

            //json
            final Record<Object> firstEventJson = createRecordEventWithKeyValuePair("test_key", "test_value");
            final Record<Object> secondEventJson = createRecordEventWithKeyValuePair("second_test_key", "second_test_value");

            expectedEventsJson.add(firstEventJson);
            expectedEventsJson.add(secondEventJson);

            // invalid json
            final String expectedInvalidJsonFirstLine = "{\"test_key: test_value\"}";
            final String expectedInvalidJsonSecondLine = "{\"second_test_key\": \"second_test_value\"";


            final Record<Object> firstEventInvalidJson = createRecordEventWithKeyValuePair(FileSource.MESSAGE_KEY, expectedInvalidJsonFirstLine);
            final Record<Object> secondEventInvalidJson = createRecordEventWithKeyValuePair(FileSource.MESSAGE_KEY, expectedInvalidJsonSecondLine);

            expectedEventsInvalidJson.add(firstEventInvalidJson);
            expectedEventsInvalidJson.add(secondEventInvalidJson);


            buffer = getBuffer();
        }

        private BlockingBuffer<Record<Object>> getBuffer() {
            final HashMap<String, Object> integerHashMap = new HashMap<>();
            integerHashMap.put("buffer_size", 2);
            integerHashMap.put("batch_size", 2);
            final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
            pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
            return new BlockingBuffer<>(pluginSetting);
        }

        @Test
        public void testFileSourceWithEmptyFilePathThrowsRuntimeException() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, "");
            fileSource = createObjectUnderTest();
            assertThrows(RuntimeException.class, () -> fileSource.start(buffer));
        }

        @Test
        public void testFileSourceWithNonexistentFilePathThrowsRuntimeException() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, FILE_DOES_NOT_EXIST);
            fileSource = createObjectUnderTest();
            assertThrows(RuntimeException.class, () -> fileSource.start(buffer));
        }

        @Test
        public void testFileSourceWithNullFilePathThrowsNullPointerException() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, null);
            assertThrows(NullPointerException.class, FileSourceTests.this::createObjectUnderTest);
        }

        @Test
        public void testFileWithPlainTextAddsEventsToBufferCorrectly() {
            fileSource = createObjectUnderTest();
            fileSource.start(buffer);

            final List<Record<Object>> bufferEvents = new ArrayList<>(buffer.read(1000).getKey());

            assertThat(bufferEvents.size(), equalTo(expectedEventsPlain.size()));
            assertExpectedRecordsAreEqual(expectedEventsPlain, bufferEvents);
        }

        @Test
        public void testFileWithJSONAddsEventsToBufferCorrectly() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, TEST_FILE_PATH_JSON);
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_FORMAT, "json");

            fileSource = createObjectUnderTest();
            fileSource.start(buffer);

            final List<Record<Object>> bufferEvents = new ArrayList<>(buffer.read(1000).getKey());

            assertThat(bufferEvents.size(), equalTo(expectedEventsJson.size()));
            assertExpectedRecordsAreEqual(expectedEventsJson, bufferEvents);
        }

        @Test
        public void testFileWithInvalidJSONAddsEventsToBufferAsPlainText() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_PATH, TEST_FILE_PATH_INVALID_JSON);
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_FORMAT, "json");
            fileSource = createObjectUnderTest();
            fileSource.start(buffer);

            final List<Record<Object>> bufferEvents = new ArrayList<>(buffer.read(1000).getKey());

            assertThat(bufferEvents.size(), equalTo(expectedEventsInvalidJson.size()));
            assertExpectedRecordsAreEqual(expectedEventsInvalidJson, bufferEvents);
        }

        @Test
        public void testStringTypeAddsStringsToBufferCorrectly() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_TYPE, FileSourceConfig.DEFAULT_TYPE);
            fileSource = createObjectUnderTest();
            fileSource.start(buffer);

            final List<Record<Object>> bufferEvents = new ArrayList<>(buffer.read(1000).getKey());

            assertThat(bufferEvents.size(), equalTo(expectedEventsPlain.size()));
            assertThat(bufferEvents.get(0).getData(), equalTo("THIS IS A PLAINTEXT LINE"));
            assertThat(bufferEvents.get(1).getData(), equalTo("THIS IS ANOTHER PLAINTEXT LINE"));

        }

        @Test
        public void testNonSupportedFileFormatThrowsIllegalArgumentException() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_FORMAT, "unsupported");
            assertThrows(IllegalArgumentException.class, FileSourceTests.this::createObjectUnderTest);
        }

        @Test
        public void testNonSupportedFileTypeThrowsIllegalArgumentException() {
            pluginSettings.put(FileSourceConfig.ATTRIBUTE_TYPE, "bad_type");
            assertThrows(IllegalArgumentException.class, FileSourceTests.this::createObjectUnderTest);
        }

        void assertExpectedRecordsAreEqual(final List<Record<Object>> expectedEvents, final List<Record<Object>> actualEvents) {
            for (int i = 0; i < expectedEvents.size(); i++) {
                assertThat(actualEvents.get(i), notNullValue());
                assertThat(actualEvents.get(i).getData(), notNullValue());
                assertEventRecordsAreEqual(actualEvents.get(i), expectedEvents.get(i));
            }
        }

        void assertEventRecordsAreEqual(final Record<Object> first, final Record<Object> second) {
            try {
                final Event firstEvent = (Event) first.getData();
                final Event secondEvent = (Event) second.getData();
                final Map<String, Object> recordMapFirst = OBJECT_MAPPER.readValue(firstEvent.toJsonString(), MAP_TYPE_REFERENCE);
                final Map<String, Object> recordMapSecond = OBJECT_MAPPER.readValue(secondEvent.toJsonString(), MAP_TYPE_REFERENCE);
                assertThat(recordMapFirst, is(equalTo(recordMapSecond)));
            } catch (JsonProcessingException e) {
                LOG.error("Unable to parse Event as JSON");
            }
        }

        private Record<Object> createRecordEventWithKeyValuePair(final String key, final String value) {
            final Map<String, Object> eventData = new HashMap<>();
            eventData.put(key, value);

            return new Record<>(TestEventFactory.getTestEventFactory().eventBuilder(EventBuilder.class)
                    .withEventType("event")
                    .withData(eventData)
                    .build());
        }
    }

    @Nested
    class WithCodec {

        @Mock
        private InputCodec inputCodec;

        @Mock
        private Buffer buffer;

        @BeforeEach
        void setUp() {
            Map<String, String> codecConfiguration = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            Map<String, Map<String, String>> codecSettings = Map.of("fake_codec", codecConfiguration);
            pluginSettings.put("codec", codecSettings);

            when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class)))
                    .thenReturn(inputCodec);
        }

        @Test
        void start_will_parse_codec_with_correct_inputStream() throws IOException {
            createObjectUnderTest().start(buffer);

            final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);

            verify(inputCodec).parse(inputStreamArgumentCaptor.capture(), any(Consumer.class));

            final InputStream actualInputStream = inputStreamArgumentCaptor.getValue();

            final byte[] actualBytes = actualInputStream.readAllBytes();
            final FileInputStream fileInputStream = new FileInputStream(TEST_FILE_PATH_PLAIN);
            final byte[] expectedBytes = fileInputStream.readAllBytes();

            assertThat(actualBytes, equalTo(expectedBytes));
        }

        @Test
        void start_will_parse_codec_with_a_Consumer_that_writes_to_the_buffer() throws IOException, TimeoutException {
            createObjectUnderTest().start(buffer);

            final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);

            verify(inputCodec).parse(any(InputStream.class), consumerArgumentCaptor.capture());

            final Consumer<Record<Event>> actualConsumer = consumerArgumentCaptor.getValue();

            final Record<Event> record = mock(Record.class);

            actualConsumer.accept(record);
            verify(buffer).write(record, FileSourceConfig.DEFAULT_TIMEOUT);
        }

        @Test
        void start_will_throw_exception_if_codec_throws() throws IOException, TimeoutException {

            final IOException mockedException = mock(IOException.class);
            doThrow(mockedException)
                    .when(inputCodec).parse(any(InputStream.class), any(Consumer.class));

            FileSource objectUnderTest = createObjectUnderTest();

            RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.start(buffer));

            assertThat(actualException.getCause(), equalTo(mockedException));
        }

    }
}
