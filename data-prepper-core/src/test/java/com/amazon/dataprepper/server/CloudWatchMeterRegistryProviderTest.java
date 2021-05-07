package com.amazon.dataprepper.server;

import com.amazon.dataprepper.pipeline.server.CloudWatchMeterRegistryProvider;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudWatchMeterRegistryProviderTest {
    private static final String TEST_CLOUDWATCH_PROPERTIES = "cloudwatch_test.properties";

    @Mock
    CloudWatchAsyncClient cloudWatchAsyncClient;

    @Mock
    CompletableFuture<PutMetricDataResponse> putMetricDataResponseCompletableFuture;


    @Before
    public void setup() throws Exception {
        when(cloudWatchAsyncClient.putMetricData(any(PutMetricDataRequest.class))).thenReturn(putMetricDataResponseCompletableFuture);
        doAnswer(ans -> null).when(putMetricDataResponseCompletableFuture).whenCompleteAsync(any());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithInvalidPropertiesFile() {
        new CloudWatchMeterRegistryProvider("does not exist", cloudWatchAsyncClient);
    }

    @Test
    public void testCreateCloudWatchMeterRegistry() {
        final CloudWatchMeterRegistryProvider cloudWatchMeterRegistryProvider = new CloudWatchMeterRegistryProvider(
                TEST_CLOUDWATCH_PROPERTIES, cloudWatchAsyncClient);
        final CloudWatchMeterRegistry cloudWatchMeterRegistry = cloudWatchMeterRegistryProvider.getCloudWatchMeterRegistry();
        assertThat(cloudWatchMeterRegistry, notNullValue());
        cloudWatchMeterRegistry.gauge("test-gauge", 1d);
        cloudWatchMeterRegistry.close(); //This will trigger publishing of metrics
        verify(cloudWatchAsyncClient, atLeast(1)).putMetricData(any(PutMetricDataRequest.class));
    }

}
