package org.opensearch.dataprepper.plugins.sink.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import static org.awaitility.Awaitility.await;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@MockitoSettings(strictness = Strictness.LENIENT)

class HttpSinkIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int NUM_RECORDS = 10;
    private static final int PORT = 8888;
    private static final int SIGV4_PORT = 8889;
    private HttpServer server;
    private HttpServer sigv4Server;
    private String serverUrl;
    private String sigv4ServerUrl;

    private  List<String> receivedRequests;
    private  List<String> sigv4ReceivedRequests;
    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private Pipeline dlqPipeline;
    
    @Mock
    private PipelineDescription pipelineDescription;
    
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter eventsSuccessCounter;
    @Mock
    private Counter eventsFailedCounter;
    @Mock
    private Counter requestsSuccessCounter;
    @Mock
    private Counter requestsFailedCounter;
    @Mock
    private Counter requestRetriesCounter;
    @Mock
    private DistributionSummary summary;
    @Mock
    private Timer timer;
    @Mock
    AwsConfig awsConfig;

    private String awsRegion;
    private String awsRole;

    @Mock
    private HttpSinkConfiguration httpSinkConfiguration;

    @BeforeEach
    void setUp() throws Exception {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        receivedRequests = new ArrayList<>();
        sigv4ReceivedRequests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress(PORT), 0);
        serverUrl = "http://localhost:" + PORT;
        server.start();

        sigv4Server = HttpServer.create(new InetSocketAddress(SIGV4_PORT), 0);
        sigv4ServerUrl = "http://localhost:" + SIGV4_PORT;
        sigv4Server.start();

        timer = mock(Timer.class);
        summary = mock(DistributionSummary.class);

        eventsSuccessCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        pluginMetrics = mock(PluginMetrics.class);

        when(pluginMetrics.counter(eq("sinkRequestsSucceeded"))).thenReturn(requestsSuccessCounter);
        when(pluginMetrics.counter(eq("sinkRequestsFailed"))).thenReturn(requestsFailedCounter);
        when(pluginMetrics.counter(eq("sinkEventsSucceeded"))).thenReturn(eventsSuccessCounter);
        when(pluginMetrics.counter(eq("sinkEventsFailed"))).thenReturn(eventsFailedCounter);
        when(pluginMetrics.counter(eq("sinkRetries"))).thenReturn(requestRetriesCounter);

        when(pluginMetrics.summary(any(String.class))).thenReturn(summary);
        when(pluginMetrics.timer(any(String.class))).thenReturn(timer);

        awsConfig = mock(AwsConfig.class);
        awsRegion = System.getProperty("tests.aws.region");
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(awsRegion));
        awsRole = System.getProperty("tests.aws.role");
        when(awsConfig.getAwsStsRoleArn()).thenReturn(awsRole);

        pluginFactory = mock(PluginFactory.class);
        JsonOutputCodec jsonCodec = new JsonOutputCodec(new JsonOutputCodecConfig());
        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(jsonCodec);
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn("test-pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(awsCredentialsSupplier.getProvider(any())).thenAnswer(options -> DefaultCredentialsProvider.create());
        
        when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");
    }

    HttpSink createObjectUnderTest(HttpSinkConfiguration config) throws IOException {
        return new HttpSink(pluginSetting, config, pluginFactory, pipelineDescription,
        null, awsCredentialsSupplier, pluginMetrics);
    }
    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
        if (sigv4Server != null) {
            sigv4Server.stop(0);
        }
        receivedRequests.clear();
        sigv4ReceivedRequests.clear();
    }

    @Test
    void testHttpSink_withSuccessResponseWithEventCountThreshold() throws Exception {
        server.createContext("/success", exchange -> {
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            receivedRequests.add(body);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfig(serverUrl + "/success", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            Event event = createEvent(Map.of("key", "value" + i));
            records.add(new Record<>(event));
        }
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    assertEquals(NUM_RECORDS, receivedRequests.size());
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        assertTrue(receivedRequests.get(i).contains("value"+i));
                    }
                });
        verify(requestsSuccessCounter, times(NUM_RECORDS)).increment(1);
        verify(eventsSuccessCounter, times(NUM_RECORDS)).increment(1);
    }

    @Test
    void testHttpSink_withSuccessResponseWithMaxSizeThresholdAndFlushTimeOut() throws Exception {
        server.createContext("/success", exchange -> {
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            receivedRequests.add(body);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        String json = "{\"max_request_size\": \"20b\", \"flush_timeout\": \"PT10S\"}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfig(serverUrl + "/success", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            Event event = createEvent(Map.of("test_key", "test_value" + i));
            records.add(new Record<>(event));
        }
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    assertEquals(NUM_RECORDS, receivedRequests.size());
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        assertTrue(receivedRequests.get(i).contains("value"+i));
                    }
                });
        verify(requestsSuccessCounter, times(NUM_RECORDS)).increment(1);
        verify(eventsSuccessCounter, times(NUM_RECORDS)).increment(1);
    }


    Event createEvent(Map<String, String> data) {
        return TestEventFactory.getTestEventFactory().eventBuilder(EventBuilder.class)
                .withData(data)
                .withEventType("event")
                .build();
    }

    @Test
    void testHttpSink_withServerError() throws Exception {
        server.createContext("/error", exchange -> {
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            receivedRequests.add(body);
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfig(serverUrl + "/error", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        Event event = createEvent(Map.of("test_key", "test_value"));
        records.add(new Record<>(event));


        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    verify(requestRetriesCounter, times(3)).increment(1);
                });
        verify(requestsFailedCounter, times(1)).increment(1);
        verify(eventsFailedCounter, times(1)).increment(1);
    }

    @Test
    void testHttpSink_withBadRequest() throws Exception {
        server.createContext("/badrequest", exchange -> {
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            receivedRequests.add(body);
            exchange.sendResponseHeaders(401, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfig(serverUrl + "/error", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        Event event = createEvent(Map.of("test_key", "test_value"));
        records.add(new Record<>(event));
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {

                            verify(requestsFailedCounter, times(1)).increment(1);
                            verify(eventsFailedCounter, times(1)).increment(1);
                        });

        verify(requestRetriesCounter, times(0)).increment(1);
    }

    private HttpSinkConfiguration createConfig(String url) {
        return createConfig(url, new ThresholdOptions());
    }

    private HttpSinkConfiguration createConfig(String url, ThresholdOptions thresholdOptions) {
        HttpSinkConfiguration config = new HttpSinkConfiguration();
        Map<String, Object> codecSettings = new HashMap<>();
        PluginModel codecModel = new PluginModel("json", codecSettings);
        
        try {
            java.lang.reflect.Field urlField = HttpSinkConfiguration.class.getDeclaredField("url");
            urlField.setAccessible(true);
            urlField.set(config, url);
            
            java.lang.reflect.Field codecField = HttpSinkConfiguration.class.getDeclaredField("codec");
            codecField.setAccessible(true);
            codecField.set(config, codecModel);
            
            java.lang.reflect.Field thresholdField = HttpSinkConfiguration.class.getDeclaredField("thresholdOptions");
            thresholdField.setAccessible(true);
            thresholdField.set(config, thresholdOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        return config;
    }

    private HttpSinkConfiguration createConfigWithSigV4(String url, ThresholdOptions thresholdOptions) {
        HttpSinkConfiguration config = new HttpSinkConfiguration();
        Map<String, Object> codecSettings = new HashMap<>();
        PluginModel codecModel = new PluginModel("json", codecSettings);
        
        try {
            java.lang.reflect.Field urlField = HttpSinkConfiguration.class.getDeclaredField("url");
            urlField.setAccessible(true);
            urlField.set(config, url);
            
            java.lang.reflect.Field codecField = HttpSinkConfiguration.class.getDeclaredField("codec");
            codecField.setAccessible(true);
            codecField.set(config, codecModel);
            
            java.lang.reflect.Field thresholdField = HttpSinkConfiguration.class.getDeclaredField("thresholdOptions");
            thresholdField.setAccessible(true);
            thresholdField.set(config, thresholdOptions);

            java.lang.reflect.Field awsField = HttpSinkConfiguration.class.getDeclaredField("awsConfig");
            awsField.setAccessible(true);
            awsField.set(config, awsConfig);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        return config;
    }

    @Test
    void testHttpSink_withSuccessResponseWithEventCountThreshold_SigV4() throws Exception {
        sigv4Server.createContext("/success", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            if (authHeader == null || !authHeader.contains("AWS4-HMAC-SHA256")) {
                exchange.sendResponseHeaders(401, 0);
                exchange.close();
                return;
            }
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            sigv4ReceivedRequests.add(body);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfigWithSigV4(sigv4ServerUrl + "/success", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            Event event = createEvent(Map.of("key", "value" + i));
            records.add(new Record<>(event));
        }
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    assertEquals(NUM_RECORDS, sigv4ReceivedRequests.size());
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        assertTrue(sigv4ReceivedRequests.get(i).contains("value"+i));
                    }
                });
        verify(requestsSuccessCounter, times(NUM_RECORDS)).increment(1);
        verify(eventsSuccessCounter, times(NUM_RECORDS)).increment(1);
    }

    @Test
    void testHttpSink_withSuccessResponseWithMaxSizeThreshold_SigV4() throws Exception {
        sigv4Server.createContext("/success", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            if (authHeader == null || !authHeader.contains("AWS4-HMAC-SHA256")) {
                exchange.sendResponseHeaders(401, 0);
                exchange.close();
                return;
            }
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            sigv4ReceivedRequests.add(body);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        String json = "{\"max_request_size\": \"20b\", \"flush_timeout\": \"PT10S\"}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfigWithSigV4(sigv4ServerUrl + "/success", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            Event event = createEvent(Map.of("test_key", "test_value" + i));
            records.add(new Record<>(event));
        }
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    assertEquals(NUM_RECORDS, sigv4ReceivedRequests.size());
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        assertTrue(sigv4ReceivedRequests.get(i).contains("value"+i));
                    }
                });
        verify(requestsSuccessCounter, times(NUM_RECORDS)).increment(1);
        verify(eventsSuccessCounter, times(NUM_RECORDS)).increment(1);
    }

    @Test
    void testHttpSink_withServerError_SigV4() throws Exception {
        sigv4Server.createContext("/error", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            if (authHeader == null || !authHeader.contains("AWS4-HMAC-SHA256")) {
                exchange.sendResponseHeaders(401, 0);
                exchange.close();
                return;
            }
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            sigv4ReceivedRequests.add(body);
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfigWithSigV4(sigv4ServerUrl + "/error", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        Event event = createEvent(Map.of("test_key", "test_value"));
        records.add(new Record<>(event));

        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    verify(requestRetriesCounter, times(3)).increment(1);
                });
        verify(requestsFailedCounter, times(1)).increment(1);
        verify(eventsFailedCounter, times(1)).increment(1);
    }

    @Test
    void testHttpSink_withDLQPipeline() throws Exception {
        dlqPipeline = mock(Pipeline.class);
        server.createContext("/badrequest", exchange -> {
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            receivedRequests.add(body);
            exchange.sendResponseHeaders(401, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfig(serverUrl + "/error", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();
        sink.setDlqPipeline(dlqPipeline);

        List<Record<Event>> records = new ArrayList<>();
        Event event = createEvent(Map.of("test_key", "test_value"));
        records.add(new Record<>(event));
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {

                    verify(requestsFailedCounter, times(1)).increment(1);
                    verify(eventsFailedCounter, times(1)).increment(1);
                });

        verify(requestRetriesCounter, times(0)).increment(1);
        verify(dlqPipeline, times(1)).sendEvents(any());
    }

    @Test
    public void testToVerifyLackOfCredentialsResultInFailure() throws Exception {
        AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(provider);
        sigv4Server.createContext("/success", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            if (authHeader == null || !authHeader.contains("AWS4-HMAC-SHA256")) {
                exchange.sendResponseHeaders(401, 0);
                exchange.close();
                return;
            }
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            sigv4ReceivedRequests.add(body);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        String json = "{\"max_events\": 1}";
        ThresholdOptions thresholdOptions = OBJECT_MAPPER.readValue(json, ThresholdOptions.class);
        HttpSinkConfiguration config = createConfigWithSigV4(sigv4ServerUrl + "/success", thresholdOptions);
        HttpSink sink = createObjectUnderTest(config);
        sink.doInitialize();

        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            Event event = createEvent(Map.of("key", "value" + i));
            records.add(new Record<>(event));
        }
        sink.doOutput(records);
        verify(requestsFailedCounter, times(NUM_RECORDS)).increment(1);
    }
}
