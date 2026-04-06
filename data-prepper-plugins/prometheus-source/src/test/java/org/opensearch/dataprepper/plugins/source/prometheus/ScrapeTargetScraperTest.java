/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ScrapeTargetScraperTest {

    private static final String METRICS_BODY =
            "# HELP test_gauge A test gauge\n" +
            "# TYPE test_gauge gauge\n" +
            "test_gauge{instance=\"localhost\"} 42.0\n";

    @Mock
    private PrometheusScrapeConfig config;

    private Server server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop().join();
        }
    }

    @Test
    void testConstructorCreatesScraperWithNoAuth() {
        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);

        assertThat(scraper, notNullValue());
    }

    @Test
    void testCommonHeadersContainAcceptHeader() throws Exception {
        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        assertThat(headers.get(HttpHeaderNames.ACCEPT), equalTo("text/plain;version=0.0.4"));
    }

    @Test
    void testCommonHeadersWithNoAuthentication() throws Exception {
        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        assertThat(headers.contains(HttpHeaderNames.AUTHORIZATION), is(false));
    }

    @Test
    void testCommonHeadersWithBasicAuth() throws Exception {
        final ScrapeAuthenticationConfig authConfig = mock(ScrapeAuthenticationConfig.class);
        final ScrapeAuthenticationConfig.HttpBasicConfig httpBasicConfig =
                mock(ScrapeAuthenticationConfig.HttpBasicConfig.class);
        when(httpBasicConfig.getUsername()).thenReturn("admin");
        when(httpBasicConfig.getPassword()).thenReturn("secret");
        when(authConfig.getHttpBasic()).thenReturn(httpBasicConfig);
        when(config.getAuthentication()).thenReturn(authConfig);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        final String expectedCredentials = Base64.getEncoder()
                .encodeToString("admin:secret".getBytes(StandardCharsets.UTF_8));
        assertThat(headers.get(HttpHeaderNames.AUTHORIZATION), equalTo("Basic " + expectedCredentials));
    }

    @Test
    void testCommonHeadersWithBearerToken() throws Exception {
        final ScrapeAuthenticationConfig authConfig = mock(ScrapeAuthenticationConfig.class);
        when(authConfig.getHttpBasic()).thenReturn(null);
        when(authConfig.getBearerToken()).thenReturn("my-jwt-token");
        when(config.getAuthentication()).thenReturn(authConfig);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        assertThat(headers.get(HttpHeaderNames.AUTHORIZATION), equalTo("Bearer my-jwt-token"));
    }

    @Test
    void testBasicAuthTakesPrecedenceOverBearerToken() throws Exception {
        final ScrapeAuthenticationConfig authConfig = mock(ScrapeAuthenticationConfig.class);
        final ScrapeAuthenticationConfig.HttpBasicConfig httpBasicConfig =
                mock(ScrapeAuthenticationConfig.HttpBasicConfig.class);
        when(httpBasicConfig.getUsername()).thenReturn("user");
        when(httpBasicConfig.getPassword()).thenReturn("pass");
        when(authConfig.getHttpBasic()).thenReturn(httpBasicConfig);
        when(config.getAuthentication()).thenReturn(authConfig);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        assertThat(headers.get(HttpHeaderNames.AUTHORIZATION), containsString("Basic "));
    }

    @Test
    void testAuthenticationWithNullBearerToken() throws Exception {
        final ScrapeAuthenticationConfig authConfig = mock(ScrapeAuthenticationConfig.class);
        when(authConfig.getHttpBasic()).thenReturn(null);
        when(authConfig.getBearerToken()).thenReturn(null);
        when(config.getAuthentication()).thenReturn(authConfig);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final HttpHeaders headers = getCommonHeaders(scraper);

        assertThat(headers.contains(HttpHeaderNames.AUTHORIZATION), is(false));
    }

    @Test
    void scrape_returns_response_body_from_successful_endpoint() {
        server = startServer("/metrics", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_uses_correct_path_from_url() {
        server = startServer("/custom/path", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/custom/path");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_defaults_to_root_path_when_url_has_no_path() {
        server = startServer("/", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port);

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_caches_web_client_for_same_base_uri() {
        server = startServer("/metrics", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        scraper.scrape("http://127.0.0.1:" + port + "/metrics");
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_includes_query_string_in_request() {
        server = startServerWithQueryCheck("/metrics", "format=text", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics?format=text");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_throws_RuntimeException_on_non_2xx_response() {
        server = startServer("/metrics", HttpStatus.INTERNAL_SERVER_ERROR, "error");
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String url = "http://127.0.0.1:" + port + "/metrics";

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> scraper.scrape(url));
        assertThat(exception.getMessage(), containsString("500"));
    }

    @Test
    void scrape_with_insecure_mode_uses_tls_no_verify_factory() {
        server = startServer("/metrics", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(true);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_with_ssl_certificate_file_uses_custom_trust_manager_factory() {
        server = startServer("/metrics", HttpStatus.OK, METRICS_BODY);
        final int port = server.activeLocalPort();

        final String certPath = getClass().getClassLoader().getResource("test_cert.crt").getFile();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(certPath);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics");

        assertThat(result, equalTo(METRICS_BODY));
    }

    @Test
    void scrape_with_default_factory_when_no_ssl_and_not_insecure() {
        server = startServer("/metrics", HttpStatus.OK, "up 1\n");
        final int port = server.activeLocalPort();

        when(config.getAuthentication()).thenReturn(null);
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        final String result = scraper.scrape("http://127.0.0.1:" + port + "/metrics");

        assertThat(result, equalTo("up 1\n"));
    }

    @Test
    void close_does_not_close_default_factory() {
        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(null);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        scraper.close();
    }

    @Test
    void close_closes_custom_factory_when_insecure() {
        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(true);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        scraper.close();
    }

    @Test
    void close_closes_custom_factory_when_ssl_certificate_configured() {
        final String certPath = getClass().getClassLoader().getResource("test_cert.crt").getFile();

        when(config.getAuthentication()).thenReturn(null);
        when(config.isInsecure()).thenReturn(false);
        when(config.getSslCertificateFile()).thenReturn(certPath);

        final ScrapeTargetScraper scraper = new ScrapeTargetScraper(config);
        scraper.close();
    }

    private Server startServer(final String path, final HttpStatus status, final String body) {
        final ServerBuilder sb = Server.builder();
        sb.http(0);
        sb.service(path, (ctx, req) ->
                HttpResponse.of(status, MediaType.PLAIN_TEXT_UTF_8, body));
        final Server s = sb.build();
        s.start().join();
        return s;
    }

    private Server startServerWithQueryCheck(final String path, final String expectedQuery,
                                              final HttpStatus status, final String body) {
        final ServerBuilder sb = Server.builder();
        sb.http(0);
        sb.service(path, (ctx, req) -> {
            final String query = ctx.query();
            if (expectedQuery.equals(query)) {
                return HttpResponse.of(status, MediaType.PLAIN_TEXT_UTF_8, body);
            }
            return HttpResponse.of(HttpStatus.BAD_REQUEST);
        });
        final Server s = sb.build();
        s.start().join();
        return s;
    }

    private HttpHeaders getCommonHeaders(final ScrapeTargetScraper scraper) throws Exception {
        final Field field = ScrapeTargetScraper.class.getDeclaredField("commonHeaders");
        field.setAccessible(true);
        return (HttpHeaders) field.get(scraper);
    }
}
