package com.amazon.dataprepper.server;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DataPrepperServerTest {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private DataPrepperServer dataPrepperServer;
    private DataPrepper dataPrepper;
    private final int port = 8080;

    private void setRegistry(PrometheusMeterRegistry prometheusMeterRegistry) {
        Metrics.globalRegistry.getRegistries().iterator().forEachRemaining(meterRegistry -> Metrics.globalRegistry.remove(meterRegistry));
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    private void setupDataPrepper() {
        dataPrepper = Mockito.mock(DataPrepper.class);
        Mockito.when(dataPrepper.getConfiguration()).thenReturn(new DataPrepperConfiguration(port, null));
    }

    @Before
    public void setup() {
        setRegistry(new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, ""));
    }

    @After
    public void stopServer() {
        dataPrepperServer.stop();
    }

    @Test
    public void testGetMetrics() throws IOException, InterruptedException, URISyntaxException {
        setupDataPrepper();
        final String scrape = UUID.randomUUID().toString();
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, scrape);
        setRegistry(prometheusMeterRegistry);
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port + "/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(scrape, response.body());
        dataPrepperServer.stop();
    }

    @Test
    public void testScrapeFailure() throws IOException, InterruptedException, URISyntaxException {
        setupDataPrepper();
        setRegistry(new PrometheusRegistryThrowingScrape(PrometheusConfig.DEFAULT));
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port + "/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testListPipelines() throws URISyntaxException, IOException, InterruptedException {
        setupDataPrepper();
        final String pipelineName = "testPipeline";
        Mockito.when(dataPrepper.getTransformationPipelines()).thenReturn(
                Collections.singletonMap(pipelineName, Mockito.mock(Pipeline.class))
        );
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port +"/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        final String expectedResponse = OBJECT_MAPPER.writeValueAsString(
                Collections.singletonMap("pipelines", Arrays.asList(
                        Collections.singletonMap("name", pipelineName)
                ))
        );
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(expectedResponse, response.body());
        dataPrepperServer.stop();
    }

    @Test
    public void testListPipelinesFailure() throws URISyntaxException, IOException, InterruptedException {
        setupDataPrepper();
        Mockito.when(dataPrepper.getTransformationPipelines()).thenThrow(new RuntimeException(""));
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port +"/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testShutdown() throws URISyntaxException, IOException, InterruptedException {
        setupDataPrepper();
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port +"/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Mockito.verify(dataPrepper).shutdown();
        dataPrepperServer.stop();
    }

    @Test
    public void testShutdownFailure() throws URISyntaxException, IOException, InterruptedException {
        setupDataPrepper();
        Mockito.doThrow(new RuntimeException("")).when(dataPrepper).shutdown();
        dataPrepperServer = new DataPrepperServer(dataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:"+ port +"/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        Mockito.verify(dataPrepper).shutdown();
        dataPrepperServer.stop();
    }

    private static class PrometheusRegistryMockScrape extends PrometheusMeterRegistry {
        final String scrape;
        public PrometheusRegistryMockScrape(PrometheusConfig config, final String scrape) {
            super(config);
            this.scrape = scrape;
        }

        @Override
        public String scrape() {
            return scrape;
        }
    }

    private static class PrometheusRegistryThrowingScrape extends PrometheusMeterRegistry {

        public PrometheusRegistryThrowingScrape(PrometheusConfig config) {
            super(config);
        }

        @Override
        public String scrape() {
            throw new RuntimeException("");
        }
    }

}
