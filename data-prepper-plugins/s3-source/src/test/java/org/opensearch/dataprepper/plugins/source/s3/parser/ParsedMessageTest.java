package org.opensearch.dataprepper.plugins.source.s3.parser;

import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.s3.S3EventBridgeNotification;
import org.opensearch.dataprepper.plugins.source.s3.S3EventNotification;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParsedMessageTest {
    private static final Random RANDOM = new Random();
    private Message message;
    private String testBucketName;
    private String testDecodedObjectKey;
    private long testSize;

    @BeforeEach
    void setUp() {
        message = mock(Message.class);
        testBucketName = UUID.randomUUID().toString();
        testDecodedObjectKey = UUID.randomUUID().toString();
        testSize = RANDOM.nextInt(1_000_000_000) + 1;
    }

    @Test
    void constructor_with_failed_parsing_throws_if_Message_is_null() {
        assertThrows(NullPointerException.class, () -> new ParsedMessage(null, true));
    }

    @Test
    void test_parsed_message_with_failed_parsing() {
        final ParsedMessage parsedMessage = new ParsedMessage(message, true);
        assertThat(parsedMessage.getMessage(), equalTo(message));
        assertThat(parsedMessage.isFailedParsing(), equalTo(true));
        assertThat(parsedMessage.isEmptyNotification(), equalTo(true));
    }

    @Test
    void toString_with_failed_parsing_and_messageId() {
        final String messageId = UUID.randomUUID().toString();
        when(message.messageId()).thenReturn(messageId);

        final ParsedMessage parsedMessage = new ParsedMessage(message, true);
        final String actualString = parsedMessage.toString();
        assertThat(actualString, notNullValue());
        assertThat(actualString, containsString(messageId));
    }

    @Test
    void toString_with_failed_parsing_and_no_messageId() {
        final ParsedMessage parsedMessage = new ParsedMessage(message, true);
        final String actualString = parsedMessage.toString();
        assertThat(actualString, notNullValue());
    }

    @Nested
    class WithS3EventNotificationRecord {
        private S3EventNotification.S3Entity s3Entity;
        private S3EventNotification.S3BucketEntity s3BucketEntity;
        private S3EventNotification.S3ObjectEntity s3ObjectEntity;
        private S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord;
        private List<S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords;
        private String testEventName;
        private DateTime testEventTime;

        @BeforeEach
        void setUp() {
            testEventName = UUID.randomUUID().toString();
            testEventTime = DateTime.now();

            s3Entity = mock(S3EventNotification.S3Entity.class);
            s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);
            s3ObjectEntity = mock(S3EventNotification.S3ObjectEntity.class);
            s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);

            when(s3EventNotificationRecord.getS3()).thenReturn(s3Entity);
            when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
            when(s3Entity.getObject()).thenReturn(s3ObjectEntity);
            when(s3ObjectEntity.getSizeAsLong()).thenReturn(testSize);
            when(s3BucketEntity.getName()).thenReturn(testBucketName);
            when(s3ObjectEntity.getUrlDecodedKey()).thenReturn(testDecodedObjectKey);
            when(s3EventNotificationRecord.getEventName()).thenReturn(testEventName);
            when(s3EventNotificationRecord.getEventTime()).thenReturn(testEventTime);

            s3EventNotificationRecords = List.of(s3EventNotificationRecord);
        }

        private ParsedMessage createObjectUnderTest() {
            return new ParsedMessage(message, s3EventNotificationRecords);
        }

        @Test
        void constructor_with_S3EventNotificationRecord_throws_if_Message_is_null() {
            message = null;
            assertThrows(NullPointerException.class, this::createObjectUnderTest);
        }

        @Test
        void test_parsed_message_with_S3EventNotificationRecord() {
            final ParsedMessage parsedMessage = createObjectUnderTest();

            assertThat(parsedMessage.getMessage(), equalTo(message));
            assertThat(parsedMessage.getBucketName(), equalTo(testBucketName));
            assertThat(parsedMessage.getObjectKey(), equalTo(testDecodedObjectKey));
            assertThat(parsedMessage.getObjectSize(), equalTo(testSize));
            assertThat(parsedMessage.getEventName(), equalTo(testEventName));
            assertThat(parsedMessage.getEventTime(), equalTo(testEventTime));
            assertThat(parsedMessage.isFailedParsing(), equalTo(false));
            assertThat(parsedMessage.isEmptyNotification(), equalTo(false));
            assertThat(parsedMessage.isShouldSkipProcessing(), equalTo(false));
        }

        @Test
        void toString_with_messageId() {
            final String messageId = UUID.randomUUID().toString();
            when(message.messageId()).thenReturn(messageId);

            final ParsedMessage parsedMessage = createObjectUnderTest();
            final String actualString = parsedMessage.toString();
            assertThat(actualString, notNullValue());
            assertThat(actualString, containsString(messageId));
            assertThat(actualString, containsString(testDecodedObjectKey));
        }

        @Test
        void test_parsed_message_with_null_object_size_defaults_to_zero() {
            when(s3ObjectEntity.getSizeAsLong()).thenReturn(null);
            final ParsedMessage parsedMessage = createObjectUnderTest();
            assertThat(parsedMessage.getObjectSize(), equalTo(0L));
        }
    }

    @Nested
    class WithS3EventBridgeNotification {
        private String testDetailType;
        private DateTime testEventTime;
        private S3EventBridgeNotification s3EventBridgeNotification;
        private S3EventBridgeNotification.Detail detail;
        private S3EventBridgeNotification.Bucket bucket;
        private S3EventBridgeNotification.Object object;

        @BeforeEach
        void setUp() {
            s3EventBridgeNotification = mock(S3EventBridgeNotification.class);
            detail = mock(S3EventBridgeNotification.Detail.class);
            bucket = mock(S3EventBridgeNotification.Bucket.class);
            object = mock(S3EventBridgeNotification.Object.class);

            testDetailType = UUID.randomUUID().toString();
            testEventTime = DateTime.now();

            when(s3EventBridgeNotification.getDetail()).thenReturn(detail);
            when(s3EventBridgeNotification.getDetail().getBucket()).thenReturn(bucket);
            when(s3EventBridgeNotification.getDetail().getObject()).thenReturn(object);

            when(bucket.getName()).thenReturn(testBucketName);
            when(object.getUrlDecodedKey()).thenReturn(testDecodedObjectKey);
            when(object.getSize()).thenReturn((int) testSize);
            when(s3EventBridgeNotification.getDetailType()).thenReturn(testDetailType);
            when(s3EventBridgeNotification.getTime()).thenReturn(testEventTime);
        }

        private ParsedMessage createObjectUnderTest() {
            return new ParsedMessage(message, s3EventBridgeNotification);
        }

        @Test
        void constructor_with_S3EventBridgeNotification_throws_if_Message_is_null() {
            message = null;
            assertThrows(NullPointerException.class, () -> createObjectUnderTest());
        }

        @Test
        void test_parsed_message_with_S3EventBridgeNotification() {
            final ParsedMessage parsedMessage = createObjectUnderTest();

            assertThat(parsedMessage.getMessage(), equalTo(message));
            assertThat(parsedMessage.getBucketName(), equalTo(testBucketName));
            assertThat(parsedMessage.getObjectKey(), equalTo(testDecodedObjectKey));
            assertThat(parsedMessage.getObjectSize(), equalTo(testSize));
            assertThat(parsedMessage.getDetailType(), equalTo(testDetailType));
            assertThat(parsedMessage.getEventTime(), equalTo(testEventTime));
            assertThat(parsedMessage.isFailedParsing(), equalTo(false));
            assertThat(parsedMessage.isEmptyNotification(), equalTo(false));
        }

        @Test
        void toString_with_messageId() {
            final String messageId = UUID.randomUUID().toString();
            when(message.messageId()).thenReturn(messageId);

            final ParsedMessage parsedMessage = createObjectUnderTest();
            final String actualString = parsedMessage.toString();
            assertThat(actualString, notNullValue());
            assertThat(actualString, containsString(messageId));
            assertThat(actualString, containsString(testDecodedObjectKey));
        }
    }
}
