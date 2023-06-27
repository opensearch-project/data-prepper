package org.opensearch.dataprepper.plugins.source.sqssource.handler;

import com.linecorp.armeria.client.retry.Backoff;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

class RawSqsMessageHandlerTest {

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);

    static final int NO_OF_RECORDS_TO_ACCUMULATE = 100;

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 2);
        integerHashMap.put("batch_size", 2);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName("pipeline");
        return new BlockingBuffer<>(pluginSetting);
    }

    @Test
    void sqs_messages_handler_will_read_sqs_message_and_push_to_buffer(){
        final BlockingBuffer<Record<Event>> buffer = getBuffer();
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        String message = UUID.randomUUID().toString();
        String messageId = UUID.randomUUID().toString();
        String receiptHandle = UUID.randomUUID().toString();
        List<Message> messageList = List.of(Message.builder().body(message).messageId(messageId).receiptHandle(receiptHandle).build());
        SqsService sqsService = new SqsService(mock(SqsMetrics.class),mock(SqsClient.class),mock(Backoff.class));
        final BufferAccumulator<Record<Event>> recordBufferAccumulator = BufferAccumulator.create(buffer, NO_OF_RECORDS_TO_ACCUMULATE, BUFFER_TIMEOUT);
        RawSqsMessageHandler rawSqsMessageHandler = new RawSqsMessageHandler(sqsService);
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = rawSqsMessageHandler.handleMessages(messageList,recordBufferAccumulator, acknowledgementSet);
        final List<Record<Event>> bufferEvents = new ArrayList<>(buffer.read((int) Duration.ofSeconds(10).toMillis()).getKey());
        final String bufferMessage = bufferEvents.get(0).getData().get("message", String.class);

        assertThat(bufferMessage, CoreMatchers.equalTo(message));

        assertThat(deleteMessageBatchRequestEntries.get(0).receiptHandle(),equalTo(receiptHandle));
        assertThat(deleteMessageBatchRequestEntries.get(0).id(),equalTo(messageId));
    }
}
