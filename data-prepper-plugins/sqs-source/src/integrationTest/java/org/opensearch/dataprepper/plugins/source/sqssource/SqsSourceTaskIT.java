/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.source.sqssource.handler.RawSqsMessageHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqsSourceTaskIT {

    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();

    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();

    static final double JITTER_RATE = 0.20;

    private static final String TEST_PIPELINE_NAME = "pipeline";

    private static final String MESSAGE = "message";

    private static final String JSON_MESSAGE = "{\"array\":[{\"name\":\"abc\",\"test\":[{\"company\":\"xyz\"}]},{\"number\":1}]}";

    private static final String LOG_MESSAGE = "2023-06-14T11:59:54,350 [main] INFO  Test - Application started Successfully\n";

    private static final String AWS_SQS_QUEUE_URL = "tests.sqs.source.queue.url";

    private static final String AWS_REGION = "tests.sqs.source.aws.region";
    public static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);
    public static final int RECORDS_TO_ACCUMULATE = 100;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private BlockingBuffer<Record<Event>> buffer;

    private Counter messageReceivedCounter;

    private Counter messageDeletedCounter;

    private Backoff backoff;

    private SqsClient sqsClient;

    private SqsMetrics sqsMetrics;

    private ScheduledExecutorService executorService;

    @ParameterizedTest
    @CsvSource({"2,1","10,2","50,4","100,5","200,7","500,10","1000,15","2000,24"})
    public void process_sqs_messages(int messageLoad,int threadSleepTime){
        final SqsRecordsGenerator sqsRecordsGenerator = new SqsRecordsGenerator(sqsClient);
        final String queueUrl = System.getProperty(AWS_SQS_QUEUE_URL);

        List<String> inputDataList = pushMessagesToQueue(sqsRecordsGenerator, queueUrl,messageLoad);
        this.buffer = getBuffer(inputDataList.size());

        SqsOptions sqsOptions = new SqsOptions.Builder().setSqsUrl(queueUrl).setMaximumMessages(10).build();
        executorService.scheduleAtFixedRate(createObjectUnderTest(sqsOptions),0,1, TimeUnit.MILLISECONDS);
        try {
            Thread.sleep(Duration.ofSeconds(threadSleepTime).toMillis());
        } catch (InterruptedException e) {
        }
        executorService.shutdown();
        final List<Record<Event>> bufferEvents = new ArrayList<>(buffer.read((int) Duration.ofSeconds(10).toMillis()).getKey());
        final List<String> bufferData = bufferEvents.stream().map(obj -> obj.getData().get(MESSAGE, String.class)).collect(Collectors.toList());
        assertThat(bufferData, containsInAnyOrder(inputDataList.toArray()));
        assertThat(bufferData.size(),equalTo(inputDataList.size()));
    }

    @BeforeEach
    public void setup(){
        this.acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        this.messageReceivedCounter = mock(Counter.class);
        this.messageDeletedCounter = mock(Counter.class);
        this.sqsMetrics = mock(SqsMetrics.class);
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);
        this.backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        this.sqsClient = SqsClient.builder().region(Region.of(System.getProperty(AWS_REGION))).build();
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public SqsSourceTask createObjectUnderTest(final SqsOptions sqsOptions){
        SqsService sqsService = new SqsService(sqsMetrics,sqsClient,backoff);
        SqsMessageHandler sqsHandler = new RawSqsMessageHandler(sqsService);
        return new SqsSourceTask(buffer, RECORDS_TO_ACCUMULATE, BUFFER_TIMEOUT
                ,sqsService,sqsOptions,sqsMetrics,
                acknowledgementSetManager,Boolean.FALSE,sqsHandler);
    }

    private static List<String> pushMessagesToQueue(SqsRecordsGenerator sqsRecordsGenerator, String queueUrl,final int load) {
        List<String> inputDataList = new ArrayList<>();
        for(int msgCount = 0; msgCount < load/2; msgCount++)
            generateMessagesForSqsPush().forEach(obj -> inputDataList.add(obj));
        sqsRecordsGenerator.pushMessages(inputDataList, queueUrl);
        return inputDataList;
    }

    private static List<String> generateMessagesForSqsPush(){
        List<String> messages = new ArrayList<>(2);
        messages.add(JSON_MESSAGE);
        messages.add(LOG_MESSAGE);
        return messages;
    }

    private BlockingBuffer<Record<Event>> getBuffer(final int bufferSize) {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", bufferSize);
        integerHashMap.put("batch_size", bufferSize);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }
}
