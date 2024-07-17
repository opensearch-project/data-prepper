/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.parser;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.s3.S3SourceConfig;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsMessageParserTest {
    @Mock
    private S3SourceConfig s3SourceConfig;

    private SqsMessageParser createObjectUnderTest() {
        return new SqsMessageParser(s3SourceConfig);
    }

    @ParameterizedTest
    @ArgumentsSource(SourceArgumentsProvider.class)
    void parseSqsMessages_returns_empty_for_empty_messages(final NotificationSourceOption sourceOption) {
        when(s3SourceConfig.getNotificationSource()).thenReturn(sourceOption);
        final Collection<ParsedMessage> parsedMessages = createObjectUnderTest().parseSqsMessages(Collections.emptyList());

        assertThat(parsedMessages, notNullValue());
        assertThat(parsedMessages, empty());
    }

    @ParameterizedTest
    @ArgumentsSource(SourceArgumentsProvider.class)
    void parseSqsMessages_parsed_messages(final NotificationSourceOption sourceOption,
                                          final String messageBody,
                                          final String replacementString) {
        when(s3SourceConfig.getNotificationSource()).thenReturn(sourceOption);
        final int numberOfMessages = 10;
        List<Message> messages = IntStream.range(0, numberOfMessages)
                .mapToObj(i -> messageBody.replaceAll(replacementString, replacementString + i))
                .map(SqsMessageParserTest::createMockMessage)
                .collect(Collectors.toList());
        final Collection<ParsedMessage> parsedMessages = createObjectUnderTest().parseSqsMessages(messages);

        assertThat(parsedMessages, notNullValue());
        assertThat(parsedMessages.size(), equalTo(numberOfMessages));

        final Set<String> bucketNames = parsedMessages.stream().map(ParsedMessage::getBucketName).collect(Collectors.toSet());
        assertThat("The bucket names are unique, so the bucketNames should match the numberOfMessages.",
                bucketNames.size(), equalTo(numberOfMessages));
    }

    static class SourceArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    Arguments.arguments(
                            NotificationSourceOption.S3,
                            S3EventNotificationParserTest.DIRECT_SQS_MESSAGE,
                            "my-bucket"),
                    Arguments.arguments(
                            NotificationSourceOption.EVENTBRIDGE,
                            S3EventBridgeNotificationParserTest.EVENTBRIDGE_MESSAGE,
                            "DOC-EXAMPLE-BUCKET1")
            );
        }
    }

    private static Message createMockMessage(final String body) {
        final Message message = mock(Message.class);
        when(message.body()).thenReturn(body);
        return message;
    }
}