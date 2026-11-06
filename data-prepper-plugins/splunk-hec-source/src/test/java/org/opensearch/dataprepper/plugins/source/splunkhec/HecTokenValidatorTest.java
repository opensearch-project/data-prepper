/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HecTokenValidatorTest {

    private String validToken;
    private String anotherToken;
    private String invalidToken;

    private HecTokenConfig tokenConfig;
    private HecTokenConfig anotherTokenConfig;
    private HecTokenConfig.HecTokenDefaults tokenDefaults;

    @BeforeEach
    void setUp() {
        validToken = UUID.randomUUID().toString();
        anotherToken = UUID.randomUUID().toString();
        invalidToken = UUID.randomUUID().toString();

        tokenConfig = mock(HecTokenConfig.class);
        when(tokenConfig.getToken()).thenReturn(validToken);
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        tokenDefaults = mock(HecTokenConfig.HecTokenDefaults.class);
        when(tokenDefaults.getIndex()).thenReturn("test-index");
        when(tokenDefaults.getSourcetype()).thenReturn("test-sourcetype");
        when(tokenConfig.getDefaults()).thenReturn(tokenDefaults);

        anotherTokenConfig = mock(HecTokenConfig.class);
        when(anotherTokenConfig.getToken()).thenReturn(anotherToken);
        lenient().when(anotherTokenConfig.isEnabled()).thenReturn(true);
        when(anotherTokenConfig.getDefaults()).thenReturn(null);
    }

    @Test
    void constructor_with_null_tokenConfigs_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () -> new HecTokenValidator(null));
    }

    @Test
    void constructor_with_null_token_value_throws_IllegalArgumentException() {
        final HecTokenConfig nullTokenConfig = mock(HecTokenConfig.class);
        when(nullTokenConfig.getToken()).thenReturn(null);
        final List<HecTokenConfig> configs = List.of(nullTokenConfig);
        assertThrows(IllegalArgumentException.class, () -> new HecTokenValidator(configs));
    }

    @Test
    void constructor_with_blank_token_value_throws_IllegalArgumentException() {
        final HecTokenConfig blankTokenConfig = mock(HecTokenConfig.class);
        when(blankTokenConfig.getToken()).thenReturn("   ");
        final List<HecTokenConfig> configs = List.of(blankTokenConfig);
        assertThrows(IllegalArgumentException.class, () -> new HecTokenValidator(configs));
    }

    @Test
    void isValid_returns_true_for_valid_token() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(tokenConfig));
        assertThat(validator.isValid(validToken), is(true));
    }

    @Test
    void isValid_returns_false_for_invalid_token() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(tokenConfig));
        assertThat(validator.isValid(invalidToken), is(false));
    }

    @Test
    void isValid_returns_false_for_null_token() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(tokenConfig));
        assertThat(validator.isValid(null), is(false));
    }

    @Test
    void isValid_with_multiple_tokens() {
        final HecTokenValidator validator = new HecTokenValidator(Arrays.asList(tokenConfig, anotherTokenConfig));
        assertThat(validator.isValid(validToken), is(true));
        assertThat(validator.isValid(anotherToken), is(true));
        assertThat(validator.isValid(invalidToken), is(false));
    }

    @Test
    void getDefaults_returns_defaults_for_token_with_defaults() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(tokenConfig));
        final Optional<HecTokenConfig.HecTokenDefaults> result = validator.getDefaults(validToken);
        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getIndex(), equalTo("test-index"));
        assertThat(result.get().getSourcetype(), equalTo("test-sourcetype"));
    }

    @Test
    void getDefaults_returns_empty_for_token_without_defaults() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(anotherTokenConfig));
        final Optional<HecTokenConfig.HecTokenDefaults> result = validator.getDefaults(anotherToken);
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void getDefaults_returns_empty_for_unknown_token() {
        final HecTokenValidator validator = new HecTokenValidator(List.of(tokenConfig));
        final Optional<HecTokenConfig.HecTokenDefaults> result = validator.getDefaults(invalidToken);
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void extractToken_returns_token_from_valid_authorization_header() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken("Splunk " + validToken);
        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), equalTo(validToken));
    }

    @Test
    void extractToken_returns_empty_for_null_header() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken(null);
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void extractToken_returns_empty_for_blank_header() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken("   ");
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void extractToken_returns_empty_for_wrong_scheme() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken("Bearer " + validToken);
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void extractToken_returns_empty_for_splunk_prefix_only() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken("Splunk ");
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void extractToken_trims_whitespace_from_token_value() {
        final HecTokenValidator validator = new HecTokenValidator(Collections.emptyList());
        final Optional<String> result = validator.extractToken("Splunk  " + validToken + "  ");
        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), equalTo(validToken));
    }

    @Test
    void disabled_token_is_marked_disabled_and_not_valid() {
        final HecTokenConfig disabledConfig = mock(HecTokenConfig.class);
        when(disabledConfig.getToken()).thenReturn(invalidToken);
        when(disabledConfig.isEnabled()).thenReturn(false);
        final HecTokenValidator validator = new HecTokenValidator(Arrays.asList(tokenConfig, disabledConfig));
        assertThat(validator.isDisabled(invalidToken), is(true));
        assertThat(validator.isValid(invalidToken), is(false));
        assertThat(validator.isDisabled(validToken), is(false));
        assertThat(validator.isDisabled(null), is(false));
    }
}
