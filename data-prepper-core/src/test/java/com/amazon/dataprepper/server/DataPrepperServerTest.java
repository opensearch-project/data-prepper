/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.server;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.TestDataProvider;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataPrepperServerTest {
    @Mock
    DataPrepperConfiguration dataPrepperConfiguration;
    @Mock
    PluginFactory pluginFactory;
    @Mock
    DataPrepper dataPrepper;
    @Mock
    DataPrepperCoreAuthenticationProvider unauthenticatedProvider;
    @Mock
    CompositeMeterRegistry systemMeterRegistry;

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private DataPrepperServer dataPrepperServer;
    private final int port = 1234;

    private void setRegistry(PrometheusMeterRegistry prometheusMeterRegistry) {
        final Set<MeterRegistry> meterRegistries = Metrics.globalRegistry.getRegistries();
        //to avoid ConcurrentModificationException
        final Object[] registeredMeterRegistries = meterRegistries.toArray();
        for (final Object meterRegistry : registeredMeterRegistries) {
            Metrics.removeRegistry((MeterRegistry) meterRegistry);
        }
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    @BeforeEach
    public void setup() {
        // Required to trust any self-signed certs, used in https tests
        Properties props = System.getProperties();
        props.setProperty("jdk.internal.httpclient.disableHostnameVerification", Boolean.TRUE.toString());

        setRegistry(new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, ""));
        pluginFactory = mock(PluginFactory.class);

        when(pluginFactory.loadPlugin(eq(DataPrepperCoreAuthenticationProvider.class),
                argThat(a -> a.getName().equals(DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME))))
                .thenReturn(unauthenticatedProvider);

//        when(dataPrepper.getPluginFactory()).thenReturn(pluginFactory);
//        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
    }

    @AfterEach
    public void stopServer() {
        if (dataPrepperServer != null) {
            dataPrepperServer.stop();
        }
    }

    @Test
    public void testGetGlobalMetrics() throws IOException, InterruptedException, URISyntaxException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        final String scrape = UUID.randomUUID().toString();
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, scrape);
        setRegistry(prometheusMeterRegistry);
        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);

        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(scrape, response.body());
        dataPrepperServer.stop();
    }

    @Test
    public void testScrapeGlobalFailure() throws IOException, InterruptedException, URISyntaxException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        setRegistry(new PrometheusRegistryThrowingScrape(PrometheusConfig.DEFAULT));
        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testGetSystemMetrics() throws IOException, InterruptedException, URISyntaxException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        final String scrape = UUID.randomUUID().toString();
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, scrape);

        when(systemMeterRegistry.getRegistries())
                .thenReturn(Set.of(prometheusMeterRegistry));

        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics/sys"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testScrapeSystemMetricsFailure() throws IOException, InterruptedException, URISyntaxException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryThrowingScrape(PrometheusConfig.DEFAULT);

        when(systemMeterRegistry.getRegistries())
                .thenReturn(Set.of(prometheusMeterRegistry));

        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics/sys"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testNonPrometheusMeterRegistry() throws Exception {
        final CloudWatchMeterRegistry cloudWatchMeterRegistry = mock(CloudWatchMeterRegistry.class);
        final CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        compositeMeterRegistry.add(cloudWatchMeterRegistry);
        final DataPrepperConfiguration config = makeConfig(VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE);
        try (final MockedStatic<DataPrepper> dataPrepperMockedStatic = Mockito.mockStatic(DataPrepper.class)) {
            dataPrepperServer = new DataPrepperServer(config, pluginFactory, dataPrepper, systemMeterRegistry);
            dataPrepperServer.start();
            HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics/sys"))
                    .GET().build();
            HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        } catch (ConnectException ex) {
            dataPrepperServer.stop();
            //there should not be any Prometheus endpoints available
            assertThat(ex.getMessage(), Matchers.is("Connection refused"));
        }
    }

    @Test
    public void testMultipleMeterRegistries() throws Exception {
        //for system metrics
        final CloudWatchMeterRegistry cloudWatchMeterRegistryForSystem = mock(CloudWatchMeterRegistry.class);
        final PrometheusMeterRegistry prometheusMeterRegistryForSystem =
                new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, UUID.randomUUID().toString());

        when(systemMeterRegistry.getRegistries())
                .thenReturn(Set.of(cloudWatchMeterRegistryForSystem, prometheusMeterRegistryForSystem));

        //for data prepper metrics
        final PrometheusMeterRegistry prometheusMeterRegistryForDataPrepper =
                new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, UUID.randomUUID().toString());
        setRegistry(prometheusMeterRegistryForDataPrepper);

        final DataPrepperConfiguration config = makeConfig(VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE);

        dataPrepperServer = new DataPrepperServer(config, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        //test prometheus registry
        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + config.getServerPort()
                + "/metrics/sys")).GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testListPipelines() throws URISyntaxException, IOException, InterruptedException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        final String pipelineName = "testPipeline";
        Mockito.when(dataPrepper.getTransformationPipelines()).thenReturn(
                Collections.singletonMap("testPipeline", null)
        );
        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        final String expectedResponse = JSON_OBJECT_MAPPER.writeValueAsString(
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
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        Mockito.when(dataPrepper.getTransformationPipelines()).thenThrow(new RuntimeException(""));
        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop();
    }

    @Test
    public void testShutdown() throws URISyntaxException, IOException, InterruptedException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);

        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Mockito.verify(dataPrepper).shutdown();
        dataPrepperServer.stop();
    }

    @Test
    public void testShutdownFailure() throws URISyntaxException, IOException, InterruptedException {
        when(dataPrepperConfiguration.getServerPort()).thenReturn(port);
        Mockito.doThrow(new RuntimeException("")).when(dataPrepper).shutdown();
        dataPrepperServer = new DataPrepperServer(dataPrepperConfiguration, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        Mockito.verify(dataPrepper).shutdown();
        dataPrepperServer.stop();
    }

    @Test
    public void testGetMetricsWithHttps() throws Exception {
        DataPrepperConfiguration config = DataPrepperServerTest.makeConfig(
                TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_TLS);
        final String scrape = UUID.randomUUID().toString();
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, scrape);
        setRegistry(prometheusMeterRegistry);
        dataPrepperServer = new DataPrepperServer(config, pluginFactory, dataPrepper, systemMeterRegistry);
        dataPrepperServer.start();

        HttpClient httpsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .sslContext(getSslContextTrustingInsecure())
                .build();

        HttpRequest request = HttpRequest.newBuilder(new URI("https://localhost:" + port + "/metrics/prometheus"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();

        HttpResponse response = httpsClient.send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(scrape, response.body());
        dataPrepperServer.stop();
    }

    @Test
    public void testTlsWithInvalidPassword() throws IOException {
        DataPrepperConfiguration config = DataPrepperServerTest.makeConfig(
                TestDataProvider.INVALID_KEYSTORE_PASSWORD_DATA_PREPPER_CONFIG_FILE);

        assertThrows(IllegalStateException.class, () -> new DataPrepperServer(config, pluginFactory, dataPrepper, systemMeterRegistry));
    }

    private SSLContext getSslContextTrustingInsecure() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        return sslContext;
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

    private static DataPrepperConfiguration makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        return OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
    }

}
