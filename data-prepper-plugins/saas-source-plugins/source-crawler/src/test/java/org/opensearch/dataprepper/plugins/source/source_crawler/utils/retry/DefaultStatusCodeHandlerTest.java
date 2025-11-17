/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class DefaultStatusCodeHandlerTest {

    @Mock
    private Runnable credentialRenewal;

    private DefaultStatusCodeHandler statusCodeHandler;

    @BeforeEach
    void setUp() {
        statusCodeHandler = new DefaultStatusCodeHandler();
    }

    @Test
    void constructor_InitializesSuccessfully() {
        final DefaultStatusCodeHandler handler = new DefaultStatusCodeHandler();
        assertThat(handler, notNullValue());
    }

    @Test
    void handleStatusCode_WithUnauthorized_RenewsCredentialsAndRetriesOnce() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.UNAUTHORIZED);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), nullValue());
        verify(credentialRenewal, times(1)).run();
    }

    @Test
    void handleStatusCode_WithForbidden_StopsWithSecurityException() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.FORBIDDEN, "Forbidden");

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), notNullValue());
        assertThat(decision.getException(), instanceOf(SecurityException.class));
        assertThat(decision.getException().getMessage(),
                equalTo("Access forbidden: 403 Forbidden"));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithNotFound_Stops() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.NOT_FOUND);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), nullValue());
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithTooManyRequests_Retries() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), nullValue());
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithServiceUnavailable_Retries() {
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.SERVICE_UNAVAILABLE);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), nullValue());
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithNullStatusCode_Stops() {
        final Exception exception = new RuntimeException("Generic error");

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), nullValue());
        verifyNoInteractions(credentialRenewal);
    }

    @ParameterizedTest
    @MethodSource("clientErrorArguments")
    void handleStatusCode_WithClientErrors_Stops(final HttpStatus status) {
        final HttpClientErrorException exception = new HttpClientErrorException(status);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        verifyNoInteractions(credentialRenewal);
    }

    @ParameterizedTest
    @MethodSource("serverErrorArguments")
    void handleStatusCode_WithServerErrors_Retries(final HttpStatus status) {
        final HttpServerErrorException exception = new HttpServerErrorException(status);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), nullValue());
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithBadRequest_Stops() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithInternalServerError_Retries() {
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithBadGateway_Retries() {
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.BAD_GATEWAY);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithGatewayTimeout_Retries() {
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.GATEWAY_TIMEOUT);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithConflict_Stops() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.CONFLICT);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithUnprocessableEntity_Stops() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.UNPROCESSABLE_ENTITY);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        verifyNoInteractions(credentialRenewal);
    }

    @Test
    void handleStatusCode_WithUnauthorizedMultipleTimes_RenewsCredentialsEachTime() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.UNAUTHORIZED);

        statusCodeHandler.handleStatusCode(exception, 0, credentialRenewal);
        statusCodeHandler.handleStatusCode(exception, 1, credentialRenewal);
        statusCodeHandler.handleStatusCode(exception, 2, credentialRenewal);

        verify(credentialRenewal, times(3)).run();
    }

    @Test
    void handleStatusCode_WithDifferentRetryCount_BehavesConsistently() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final RetryDecision decision1 = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);
        final RetryDecision decision2 = statusCodeHandler.handleStatusCode(exception, 3,
                credentialRenewal);
        final RetryDecision decision3 = statusCodeHandler.handleStatusCode(exception, 5,
                credentialRenewal);

        assertThat(decision1.isShouldStop(), equalTo(false));
        assertThat(decision2.isShouldStop(), equalTo(false));
        assertThat(decision3.isShouldStop(), equalTo(false));
    }

    @Test
    void handleStatusCode_WithForbiddenAndCustomMessage_IncludesMessageInException() {
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.FORBIDDEN, "Custom forbidden message");

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException().getMessage(),
                equalTo("Access forbidden: 403 Custom forbidden message"));
    }

    @Test
    void handleStatusCode_WithHttpClientErrorExceptionAsGenericException_HandlesCorrectly() {
        final Exception exception = new HttpClientErrorException(HttpStatus.BAD_REQUEST);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(true));
    }

    @Test
    void handleStatusCode_WithHttpServerErrorExceptionAsGenericException_HandlesCorrectly() {
        final Exception exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final RetryDecision decision = statusCodeHandler.handleStatusCode(exception, 0,
                credentialRenewal);

        assertThat(decision.isShouldStop(), equalTo(false));
    }

    private static Stream<Arguments> clientErrorArguments() {
        return Stream.of(
                Arguments.of(HttpStatus.BAD_REQUEST),
                Arguments.of(HttpStatus.PAYMENT_REQUIRED),
                Arguments.of(HttpStatus.METHOD_NOT_ALLOWED),
                Arguments.of(HttpStatus.NOT_ACCEPTABLE),
                Arguments.of(HttpStatus.CONFLICT),
                Arguments.of(HttpStatus.GONE),
                Arguments.of(HttpStatus.UNPROCESSABLE_ENTITY));
    }

    private static Stream<Arguments> serverErrorArguments() {
        return Stream.of(
                Arguments.of(HttpStatus.INTERNAL_SERVER_ERROR),
                Arguments.of(HttpStatus.BAD_GATEWAY),
                Arguments.of(HttpStatus.GATEWAY_TIMEOUT),
                Arguments.of(HttpStatus.HTTP_VERSION_NOT_SUPPORTED));
    }
}
