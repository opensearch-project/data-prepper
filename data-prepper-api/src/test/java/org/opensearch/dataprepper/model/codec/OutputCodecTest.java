package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class OutputCodecTest {
    @Test
    void isCompressionInternal_returns_false() {
        OutputCodec objectUnderTest = mock(OutputCodec.class, InvocationOnMock::callRealMethod);

        assertThat(objectUnderTest.isCompressionInternal(), equalTo(false));
    }

    @Test
    void validateAgainstCodecContext_does_not_throw_or_interact_with_outputCodecContext() {
        OutputCodec objectUnderTest = mock(OutputCodec.class, InvocationOnMock::callRealMethod);

        OutputCodecContext outputCodecContext = mock(OutputCodecContext.class);

        objectUnderTest.validateAgainstCodecContext(outputCodecContext);

        verifyNoInteractions(outputCodecContext);
    }

    @Test
    public void testWriteMetrics() throws JsonProcessingException, IOException {
        OutputCodec outputCodec = new OutputCodec() {
            @Override
            public void start(OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
            }

            @Override
            public void writeEvent(Event event, OutputStream outputStream) throws IOException {
            }

            @Override
            public void complete(OutputStream outputStream) throws IOException {
            }

            @Override
            public String getExtension() {
                return null;
            }
        };

        final Set<String> testTags = Set.of("tag1");
        final EventMetadata defaultEventMetadata = DefaultEventMetadata.builder().
                withEventType(EventType.LOG.toString()).
                withTags(testTags).build();
        Map<String, Object> json = generateJson();
        final JacksonEvent event = JacksonLog.builder().withData(json).withEventMetadata(defaultEventMetadata).build();
        Event tagsToEvent = outputCodec.addTagsToEvent(event, "Tag");
        assertNotEquals(event.toJsonString(), tagsToEvent.toJsonString());
        assertThat(outputCodec.getEstimatedSize(event, new OutputCodecContext()), equalTo(0L));
    }

    private static Map<String, Object> generateJson() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 2; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        return jsonObject;
    }
}
