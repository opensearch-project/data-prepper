/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.MessageInterpolator;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig.DEFAULT_POLLING_FREQUENCY;

class FeedSourceConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer()));

    // Pass-through interpolator so the provider does not require a jakarta EL
    // implementation (which is not on the test classpath).
    private final MessageInterpolator interpolator = new MessageInterpolator() {
        @Override
        public String interpolate(final String messageTemplate, final Context context) {
            return messageTemplate;
        }

        @Override
        public String interpolate(final String messageTemplate, final Context context, final Locale locale) {
            return messageTemplate;
        }
    };

    private final Validator validator = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(interpolator)
            .buildValidatorFactory()
            .getValidator();

    @Test
    void defaults_are_applied() {
        final FeedSourceConfig config = new FeedSourceConfig();
        assertThat(config.getPollingFrequency(), equalTo(DEFAULT_POLLING_FREQUENCY));
        assertThat(config.getWorkers(), equalTo(1));
        assertThat(config.getRequestTimeout(), equalTo(Duration.ofSeconds(30)));
    }

    @Test
    void request_timeout_override_is_applied() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"request_timeout\":\"10s\"}",
                FeedSourceConfig.class);
        assertThat(config.getRequestTimeout(), equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void deserializes_feeds_map_and_globals() throws Exception {
        final String json = "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"},\"b\":{\"url\":\"https://b/f\"}},"
                + "\"polling_frequency\":\"PT10M\",\"workers\":3}";
        final FeedSourceConfig config = objectMapper.readValue(json, FeedSourceConfig.class);
        assertThat(config.getFeeds().size(), equalTo(2));
        assertThat(config.getFeeds().get("a").getUrl(), equalTo("https://a/f"));
        assertThat(config.getFeeds().get("b").getUrl(), equalTo("https://b/f"));
        assertThat(config.getPollingFrequency(), equalTo(Duration.ofMinutes(10)));
        assertThat(config.getWorkers(), equalTo(3));
    }

    @Test
    void resolvePollingFrequency_uses_feed_override_then_falls_back_to_global() {
        final FeedSourceConfig config = new FeedSourceConfig();
        final FeedConfig withOverride = new FeedConfig() {
            @Override
            public Duration getPollingFrequency() {
                return Duration.ofMinutes(1);
            }
        };
        final FeedConfig noOverride = new FeedConfig();
        assertThat(config.resolvePollingFrequency(withOverride), equalTo(Duration.ofMinutes(1)));
        assertThat(config.resolvePollingFrequency(noOverride), equalTo(DEFAULT_POLLING_FREQUENCY));
    }

    @Test
    void polling_frequency_at_or_above_one_second_is_valid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"polling_frequency\":\"1s\"}",
                FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(true));
    }

    @Test
    void global_polling_frequency_below_one_second_is_invalid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"polling_frequency\":\"500ms\"}",
                FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(false));
    }

    @Test
    void per_feed_polling_frequency_below_one_second_is_invalid() throws Exception {
        final String json = "{\"feeds\":{\"a\":{\"url\":\"https://a/f\",\"polling_frequency\":\"0s\"}},"
                + "\"polling_frequency\":\"PT5M\"}";
        final FeedSourceConfig config = objectMapper.readValue(json, FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(false));
    }

    @Test
    void feed_names_with_valid_characters_are_valid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"opensearch-forum_1\":{\"url\":\"https://a/f\"}}}", FeedSourceConfig.class);
        assertThat(config.isFeedNamesValid(), equalTo(true));
    }

    @Test
    void feed_name_with_invalid_characters_is_invalid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"bad name!\":{\"url\":\"https://a/f\"}}}", FeedSourceConfig.class);
        assertThat(config.isFeedNamesValid(), equalTo(false));
    }

    @Test
    void feed_name_exceeding_max_length_is_invalid() throws Exception {
        final String longName = "a".repeat(65);
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"" + longName + "\":{\"url\":\"https://a/f\"}}}", FeedSourceConfig.class);
        assertThat(config.isFeedNamesValid(), equalTo(false));
    }

    @Test
    void valid_config_produces_no_constraint_violations() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"workers\":2}", FeedSourceConfig.class);
        assertThat(validator.validate(config), hasSize(0));
    }

    @Test
    void empty_feeds_map_violates_not_empty_constraint() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue("{\"feeds\":{}}", FeedSourceConfig.class);
        final Set<ConstraintViolation<FeedSourceConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("feeds"));
    }

    @Test
    void workers_below_minimum_violates_min_constraint() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"workers\":0}", FeedSourceConfig.class);
        final Set<ConstraintViolation<FeedSourceConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("workers"));
    }

    @Test
    void workers_above_maximum_violates_max_constraint() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"workers\":1001}", FeedSourceConfig.class);
        final Set<ConstraintViolation<FeedSourceConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("workers"));
    }

    @Test
    void blank_feed_url_violates_not_blank_constraint() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"\"}}}", FeedSourceConfig.class);
        final Set<ConstraintViolation<FeedSourceConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("feeds[a].url"));
    }

    @Test
    void blank_basic_auth_credentials_are_cascaded_and_violate_constraints() throws Exception {
        final String json = "{\"feeds\":{\"a\":{\"url\":\"https://a/f\","
                + "\"authentication\":{\"basic\":{\"username\":\"\",\"password\":\"\"}}}}}";
        final FeedSourceConfig config = objectMapper.readValue(json, FeedSourceConfig.class);
        final Set<ConstraintViolation<FeedSourceConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(2));
        final Set<String> paths = violations.stream()
                .map(violation -> violation.getPropertyPath().toString())
                .collect(Collectors.toSet());
        // Assert the @Valid cascade reached BasicAuthConfig, not just that two violations occurred.
        assertThat(paths, containsInAnyOrder(
                "feeds[a].authentication.basic.username",
                "feeds[a].authentication.basic.password"));
    }
}
