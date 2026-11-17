/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GcpCredentialsOptionsTest {
    private static final String PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n";
    private static final String CLIENT_EMAIL = "sa@p.iam.gserviceaccount.com";

    @Test
    void build_with_no_values_uses_defaults() {
        final GcpCredentialsOptions objectUnderTest = GcpCredentialsOptions.builder().build();

        assertThat(objectUnderTest.getAuthMode(), nullValue());
        assertThat(objectUnderTest.getScopes(), equalTo(List.of(GcpCredentialsOptions.DEFAULT_SCOPE)));
        assertThat(objectUnderTest.getTokenUri(), equalTo(GcpCredentialsOptions.DEFAULT_TOKEN_URI));
        assertThat(objectUnderTest.getPrivateKey(), nullValue());
        assertThat(objectUnderTest.getClientEmail(), nullValue());
        assertThat(objectUnderTest.getProjectId(), nullValue());
    }

    @Test
    void build_with_empty_scopes_uses_default_scope() {
        assertThat(GcpCredentialsOptions.builder().withScopes(List.of()).build().getScopes(),
                equalTo(List.of(GcpCredentialsOptions.DEFAULT_SCOPE)));
    }

    @Test
    void build_service_account_key_keeps_all_values() {
        final GcpCredentialsOptions objectUnderTest = GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY)
                .withPrivateKey(PRIVATE_KEY)
                .withClientEmail(CLIENT_EMAIL)
                .withTokenUri("https://oauth2.googleapis.com/token")
                .withScopes(List.of("scope-a"))
                .withProjectId("p")
                .build();

        assertThat(objectUnderTest.getAuthMode(), equalTo(GcpAuthMode.SERVICE_ACCOUNT_KEY));
        assertThat(objectUnderTest.getPrivateKey(), equalTo(PRIVATE_KEY));
        assertThat(objectUnderTest.getClientEmail(), equalTo(CLIENT_EMAIL));
        assertThat(objectUnderTest.getScopes(), equalTo(List.of("scope-a")));
        assertThat(objectUnderTest.getProjectId(), equalTo("p"));
    }

    @Test
    void build_adc_with_key_fields_throws() {
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.ADC).withClientEmail(CLIENT_EMAIL).build());
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.ADC).withPrivateKey(PRIVATE_KEY).build());
    }

    @Test
    void build_key_fields_without_mode_throws() {
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder().withPrivateKey(PRIVATE_KEY).build());
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder().withClientEmail(CLIENT_EMAIL).build());
    }

    @Test
    void build_service_account_key_missing_a_field_throws() {
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withPrivateKey(PRIVATE_KEY).build());
        assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withClientEmail(CLIENT_EMAIL).build());
    }

    @Test
    void equals_and_hashCode_use_all_fields() {
        final GcpCredentialsOptions first = GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).withProjectId("p").build();
        final GcpCredentialsOptions same = GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).withProjectId("p").build();
        final GcpCredentialsOptions different = GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).withProjectId("q").build();

        assertThat(first, equalTo(same));
        assertThat(first.hashCode(), equalTo(same.hashCode()));
        assertThat(first, not(equalTo(different)));
        assertThat(first.equals(first), equalTo(true));
        assertThat(first.equals(null), equalTo(false));
        assertThat(first.equals("not options"), equalTo(false));
    }

    @Test
    void toString_does_not_include_private_key() {
        final GcpCredentialsOptions objectUnderTest = GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withPrivateKey(PRIVATE_KEY).withClientEmail(CLIENT_EMAIL).build();

        assertThat(objectUnderTest.toString(), containsString(CLIENT_EMAIL));
        assertThat(objectUnderTest.toString(), not(containsString("PRIVATE KEY")));
    }

    private static GcpCredentialsOptions.Builder keyBuilder() {
        return GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY)
                .withPrivateKey(PRIVATE_KEY).withClientEmail(CLIENT_EMAIL).withTokenUri("https://oauth2.googleapis.com/token")
                .withScopes(List.of("scope-a")).withProjectId("p");
    }

    static Stream<Arguments> optionsDifferingInOneField() {
        return Stream.of(
                Arguments.of(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).withScopes(List.of("scope-a"))
                        .withProjectId("p").build()),
                Arguments.of(keyBuilder().withPrivateKey("other-key").build()),
                Arguments.of(keyBuilder().withClientEmail("other@p.iam.gserviceaccount.com").build()),
                Arguments.of(keyBuilder().withTokenUri("https://oauth2.googleapis.com/other").build()),
                Arguments.of(keyBuilder().withScopes(List.of("scope-b")).build()),
                Arguments.of(keyBuilder().withProjectId("q").build()));
    }

    @ParameterizedTest
    @MethodSource("optionsDifferingInOneField")
    void equals_is_false_when_any_single_field_differs(final GcpCredentialsOptions other) {
        assertThat(keyBuilder().build(), not(equalTo(other)));
    }

    @Test
    void build_adc_with_token_uri_throws() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.ADC).withTokenUri("https://oauth2.googleapis.com/token").build());

        assertThat(exception.getMessage(), equalTo(GcpCredentialsRules.ADC_WITH_KEY_FIELDS));
    }

    @Test
    void build_token_uri_without_mode_throws() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> GcpCredentialsOptions.builder().withTokenUri("https://oauth2.googleapis.com/token").build());

        assertThat(exception.getMessage(), equalTo(GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE));
    }

    @Test
    void build_service_account_key_with_blank_fields_throws() {
        final IllegalArgumentException blankKey = assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withPrivateKey(" ").withClientEmail(CLIENT_EMAIL).build());
        final IllegalArgumentException blankEmail = assertThrows(IllegalArgumentException.class, () -> GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withPrivateKey(PRIVATE_KEY).withClientEmail("").build());

        assertThat(blankKey.getMessage(), equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
        assertThat(blankEmail.getMessage(), equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void build_sorts_and_deduplicates_scopes_so_order_does_not_change_equality() {
        final GcpCredentialsOptions first = GcpCredentialsOptions.builder().withScopes(List.of("b", "a", "b")).build();
        final GcpCredentialsOptions second = GcpCredentialsOptions.builder().withScopes(List.of("a", "b")).build();

        assertThat(first.getScopes(), equalTo(List.of("a", "b")));
        assertThat(first, equalTo(second));
    }

    @Test
    void withIdentityFrom_uses_identity_credentials_and_keeps_own_scopes_and_project() {
        final GcpCredentialsOptions plugin = GcpCredentialsOptions.builder().withScopes(List.of("read-only")).withProjectId("plugin").build();
        final GcpCredentialsOptions identity = keyBuilder().withTokenUri("https://oauth2.googleapis.com/custom").build();

        final GcpCredentialsOptions result = plugin.withIdentityFrom(identity);

        assertThat(result.getAuthMode(), equalTo(GcpAuthMode.SERVICE_ACCOUNT_KEY));
        assertThat(result.getPrivateKey(), equalTo(PRIVATE_KEY));
        assertThat(result.getClientEmail(), equalTo(CLIENT_EMAIL));
        assertThat(result.getTokenUri(), equalTo("https://oauth2.googleapis.com/custom"));
        assertThat(result.getScopes(), equalTo(List.of("read-only")));
        assertThat(result.getProjectId(), equalTo("plugin"));
    }

    @Test
    void withIdentityFrom_without_own_scopes_or_project_uses_identity_values() {
        final GcpCredentialsOptions result = GcpCredentialsOptions.builder().build().withIdentityFrom(keyBuilder().build());

        assertThat(result, equalTo(keyBuilder().build()));
    }

    @Test
    void withIdentityFrom_adc_identity_without_scopes_uses_default_scope() {
        final GcpCredentialsOptions result = GcpCredentialsOptions.builder().withProjectId("plugin").build()
                .withIdentityFrom(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).build());

        assertThat(result, equalTo(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).withProjectId("plugin").build()));
    }
}
