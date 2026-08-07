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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class PrometheusScrapeConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void testDefaultScrapeInterval() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.getScrapeInterval(), equalTo(Duration.ofSeconds(15)));
    }

    @Test
    void testDefaultScrapeTimeout() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.getScrapeTimeout(), equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void testDefaultFlattenLabels() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.isFlattenLabels(), is(false));
    }

    @Test
    void testDefaultInsecure() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.isInsecure(), is(false));
    }

    @Test
    void testDefaultTargetsIsNull() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.getTargets(), nullValue());
    }

    @Test
    void testDefaultAuthenticationIsNull() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.getAuthentication(), nullValue());
    }

    @Test
    void testDefaultSslCertificateFileIsNull() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.getSslCertificateFile(), nullValue());
    }

    @Test
    void testDeserializationFromJson() throws Exception {
        final String json = "{"
                + "\"targets\": [{\"url\": \"http://localhost:9090/metrics\"}],"
                + "\"scrape_interval\": \"PT30S\","
                + "\"scrape_timeout\": \"PT5S\","
                + "\"flatten_labels\": true,"
                + "\"insecure\": true,"
                + "\"ssl_certificate_file\": \"/path/to/cert.pem\","
                + "\"authentication\": {\"bearer_token\": \"my-token\"}"
                + "}";

        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);

        assertThat(config.getTargets(), notNullValue());
        assertThat(config.getTargets(), hasSize(1));
        assertThat(config.getTargets().get(0).getUrl(), equalTo("http://localhost:9090/metrics"));
        assertThat(config.getScrapeInterval(), equalTo(Duration.ofSeconds(30)));
        assertThat(config.getScrapeTimeout(), equalTo(Duration.ofSeconds(5)));
        assertThat(config.isFlattenLabels(), is(true));
        assertThat(config.isInsecure(), is(true));
        assertThat(config.getSslCertificateFile(), equalTo("/path/to/cert.pem"));
        assertThat(config.getAuthentication(), notNullValue());
        assertThat(config.getAuthentication().getBearerToken(), equalTo("my-token"));
    }

    @Test
    void testDeserializationWithDefaultValues() throws Exception {
        final String json = "{\"targets\": [{\"url\": \"http://host:9090/metrics\"}]}";

        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);

        assertThat(config.getScrapeInterval(), equalTo(Duration.ofSeconds(15)));
        assertThat(config.getScrapeTimeout(), equalTo(Duration.ofSeconds(10)));
        assertThat(config.isFlattenLabels(), is(false));
        assertThat(config.isInsecure(), is(false));
        assertThat(config.getSslCertificateFile(), nullValue());
        assertThat(config.getAuthentication(), nullValue());
    }

    @Test
    void testDeserializationWithMultipleTargets() throws Exception {
        final String json = "{"
                + "\"targets\": ["
                + "  {\"url\": \"http://host1:9090/metrics\"},"
                + "  {\"url\": \"http://host2:9090/metrics\"}"
                + "]"
                + "}";

        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);

        assertThat(config.getTargets(), hasSize(2));
        assertThat(config.getTargets().get(0).getUrl(), equalTo("http://host1:9090/metrics"));
        assertThat(config.getTargets().get(1).getUrl(), equalTo("http://host2:9090/metrics"));
    }

    @Test
    void isTargetUrlSchemeValid_returns_true_when_insecure() throws Exception {
        final String json = "{\"targets\": [{\"url\": \"http://host:9090/metrics\"}], \"insecure\": true}";
        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);
        assertThat(config.isTargetUrlSchemeValid(), is(true));
    }

    @Test
    void isTargetUrlSchemeValid_returns_true_when_targets_null() {
        final PrometheusScrapeConfig config = new PrometheusScrapeConfig();
        assertThat(config.isTargetUrlSchemeValid(), is(true));
    }

    @Test
    void isTargetUrlSchemeValid_returns_false_when_http_and_not_insecure() throws Exception {
        final String json = "{\"targets\": [{\"url\": \"http://host:9090/metrics\"}], \"insecure\": false}";
        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);
        assertThat(config.isTargetUrlSchemeValid(), is(false));
    }

    @Test
    void isTargetUrlSchemeValid_returns_true_when_https_and_not_insecure() throws Exception {
        final String json = "{\"targets\": [{\"url\": \"https://host:9090/metrics\"}], \"insecure\": false}";
        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);
        assertThat(config.isTargetUrlSchemeValid(), is(true));
    }

    @Test
    void isTargetUrlSchemeValid_returns_true_when_target_url_null() throws Exception {
        final String json = "{\"targets\": [{}], \"insecure\": false}";
        final PrometheusScrapeConfig config = OBJECT_MAPPER.readValue(json, PrometheusScrapeConfig.class);
        assertThat(config.isTargetUrlSchemeValid(), is(true));
    }
}