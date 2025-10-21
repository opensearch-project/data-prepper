/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.Sink;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public interface OutputCodec {

    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * A writer specific to a single buffer.
     *
     * @since 2.12
     */
    interface Writer {
        /**
         * Writes a single event to the {@link OutputStream}.
         *
         * @param event A Data Prepper {@link Event}
         * @throws IOException An IO exception writing to the stream
         *
         * @since 2.12
         */
        void writeEvent(Event event) throws IOException;

        /**
         * Completes a writer.
         *
         * @throws IOException An IO exception completing the stream
         *
         * @since 2.12
         */
        void complete() throws IOException;
    }

    /**
     * Creates a new {@link Writer} for a given {@link OutputStream}.
     * Typically, you create one per buffer.
     *
     * @param outputStream The {@link OutputStream} to write to
     * @param sampleEvent A sample Data Prepper {@link Event}.
     *                    It is not written to the stream, but may be used for metadata.
     * @param codecContext The {@link OutputCodecContext}
     * @return A {@link Writer} to use for this buffer.
     * @throws IOException An IO exception occurs initializing the writer or stream
     *
     * @since 2.12
     */
    default Writer createWriter(final OutputStream outputStream, final Event sampleEvent, final OutputCodecContext codecContext) throws IOException {
        final OutputCodec codec = this;
        codec.start(outputStream, sampleEvent, codecContext);
        return new Writer() {
            @Override
            public void writeEvent(final Event event) throws IOException {
                codec.writeEvent(event, outputStream);
            }

            @Override
            public void complete() throws IOException {
                codec.complete(outputStream);
            }
        };
    }

    /**
     * this method get called from {@link Sink} to do initial wrapping in {@link OutputStream}
     * Implementors should do initial wrapping according to the implementation
     *
     * @param outputStream  outputStream param for wrapping
     * @param event         Event to auto-generate schema
     * @param context       Extra Context used in Codec.
     * @throws IOException  throws IOException when invalid input is received or not able to create wrapping
     * @deprecated Use {@link OutputCodec#createWriter(OutputStream, Event, OutputCodecContext)} instead.
     */
    @Deprecated
    void start(OutputStream outputStream, Event event, OutputCodecContext context) throws IOException;

    /**
     * this method get called from {@link Sink} to write event in {@link OutputStream}
     * Implementors should do get data from event and write to the {@link OutputStream}
     *
     * @param event         event Record event
     * @param outputStream  outputStream param to hold the event data
     * @throws IOException throws IOException when not able to write data to {@link OutputStream}
     * @deprecated @deprecated Use {@link OutputCodec.Writer#writeEvent(Event)} instead.
     */
    @Deprecated
    void writeEvent(Event event, OutputStream outputStream) throws IOException;

    /**
     * this method get called from {@link Sink} to do final wrapping in {@link OutputStream}
     * Implementors should do final wrapping according to the implementation
     *
     * @param outputStream outputStream param for wrapping
     * @throws IOException throws IOException when invalid input is received or not able to create wrapping
     * @deprecated @deprecated Use {@link Writer#complete()} instead.
     */
    @Deprecated
    void complete(OutputStream outputStream) throws IOException;

    /**
     * this method get called from {@link Sink} to estimate size of event in {@link OutputStream}
     *
     * @param event        event Record event
     * @return long        size of the serialized event
     * @throws IOException throws IOException when invalid input is received or not able to create wrapping
     */
    default long getEstimatedSize(Event event, OutputCodecContext codecContext) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        start(outputStream, event, codecContext);
        writeEvent(event, outputStream);
        complete(outputStream);
        return outputStream.toByteArray().length;
    }

    /**
     * used to get extension of file
     *
     * @return String
     */
    String getExtension();

    /**
     * Returns true if this codec has an internal compression. That is, the entire
     * {@link OutputStream} should not be compressed.
     * <p>
     * When this value is true, sinks should not attempt to encrypt the final {@link OutputStream}
     * at all.
     * <p>
     * For example, Parquet compression happens within the file. Each column chunk
     * is compressed independently.
     *
     * @return True if the compression is internal to the codec; false if whole-file compression is ok.
     */
    default boolean isCompressionInternal() {
        return false;
    }

    default void validateAgainstCodecContext(OutputCodecContext outputCodecContext) { }

    default Event addTagsToEvent(Event event, String tagsTargetKey) throws JsonProcessingException {
        String eventJsonString = event.jsonBuilder().includeTags(tagsTargetKey).toJsonString();
        Map<String, Object> eventData = objectMapper.readValue(eventJsonString, new TypeReference<>() {
        });
        return JacksonLog.builder().withData(eventData).build();
    }
}
