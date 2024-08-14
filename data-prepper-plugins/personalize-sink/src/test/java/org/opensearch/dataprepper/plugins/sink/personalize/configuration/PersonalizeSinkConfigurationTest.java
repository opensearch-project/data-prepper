package org.opensearch.dataprepper.plugins.sink.personalize.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.personalize.dataset.DatasetTypeOptions;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersonalizeSinkConfigurationTest {
    private static final int DEFAULT_RETRIES = 10;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void getDatasetType_returns_value_from_deserialized_JSON() {
        final String datasetType = "users";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getDatasetType(), equalTo(DatasetTypeOptions.USERS));
    }

    @Test
    void getDatasetArn_returns_null_when_datasetArn_is_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getDatasetArn(), nullValue());
    }

    @Test
    void getDatasetArn_returns_value_from_deserialized_JSON() {
        final String datasetArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getDatasetArn(), equalTo(datasetArn));
    }

    @Test
    void isDatasetArnProvidedWhenNeeded_returns_true_when_datasetType_is_interactions_and_datasetArn_is_null() {
        final String datasetType = "interactions";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isDatasetArnProvidedWhenNeeded());
    }

    @Test
    void isDatasetArnProvidedWhenNeeded_returns_true_when_datasetType_is_users_and_datasetArn_is_provided() {
        final String datasetType = "users";
        final String datasetArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType, "dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isDatasetArnProvidedWhenNeeded());
    }

    @Test
    void isDatasetArnProvidedWhenNeeded_returns_false_when_datasetType_is_users_and_datasetArn_is_not_provided() {
        final String datasetType = "users";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertFalse(objectUnderTest.isDatasetArnProvidedWhenNeeded());
    }

    @Test
    void isDatasetArnProvidedWhenNeeded_returns_true_when_datasetType_is_items_and_datasetArn_is_provided() {
        final String datasetType = "items";
        final String datasetArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType, "dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isDatasetArnProvidedWhenNeeded());
    }

    @Test
    void isDatasetArnProvidedWhenNeeded_returns_false_when_datasetType_is_items_and_datasetArn_is_not_provided() {
        final String datasetType = "items";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertFalse(objectUnderTest.isDatasetArnProvidedWhenNeeded());
    }

    @Test
    void isValidDatasetArn_returns_true_for_valid_dataset_arn() {
        final String datasetArn = "arn:aws:personalize::123456789012:dataset/test";
        final Map<String, Object> jsonMap = Map.of("dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isValidDatasetArn());
    }

    @Test
    void isValidDatasetArn_returns_false_when_arn_service_is_not_personalize() {
        final String datasetArn = "arn:aws:iam::123456789012:dataset/test";
        final Map<String, Object> jsonMap = Map.of("dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertFalse(objectUnderTest.isValidDatasetArn());
    }

    @Test
    void isValidDatasetArn_returns_false_when_arn_resource_is_not_dataset() {
        final String datasetArn = "arn:aws:personalize::123456789012:role/test";
        final Map<String, Object> jsonMap = Map.of("dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertFalse(objectUnderTest.isValidDatasetArn());
    }

    @Test
    void isValidStsRoleArn_invalid_arn_throws_IllegalArgumentException() {
        final String datasetArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("dataset_arn", datasetArn);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.isValidDatasetArn());
    }



    @Test
    void getTrackingId_returns_null_when_trackingId_is_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getTrackingId(), nullValue());
    }

    @Test
    void getTrackingId_returns_value_from_deserialized_JSON() {
        final String trackingId = UUID.randomUUID().toString();;
        final Map<String, Object> jsonMap = Map.of("tracking_id", trackingId);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getTrackingId(), equalTo(trackingId));
    }

    @Test
    void isTrackingIdProvidedWhenNeeded_returns_false_when_datasetType_is_interactions_and_trackingId_is_not_provided() {
        final String datasetType = "interactions";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertFalse(objectUnderTest.isTrackingIdProvidedWhenNeeded());
    }

    @Test
    void isTrackingIdProvidedWhenNeeded_returns_true_when_datasetType_is_interactions_and_trackingId_is_provided() {
        final String datasetType = "interactions";
        final String trackingId = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType, "tracking_id", trackingId);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isTrackingIdProvidedWhenNeeded());
    }

    @Test
    void isTrackingIdProvidedWhenNeeded_returns_true_when_datasetType_is_users_and_trackingId_is_not_provided() {
        final String datasetType = "users";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isTrackingIdProvidedWhenNeeded());
    }

    @Test
    void isTrackingIdProvidedWhenNeeded_returns_true_when_datasetType_is_items_and_trackingId_is_not_provided() {
        final String datasetType = "items";
        final Map<String, Object> jsonMap = Map.of("dataset_type", datasetType);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertTrue(objectUnderTest.isTrackingIdProvidedWhenNeeded());
    }


    @Test
    void getDocumentRootKey_returns_null_when_documentRootKey_is_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getDocumentRootKey(), nullValue());
    }

    @Test
    void getDocumentRootKey_returns_value_from_deserialized_JSON() {
        final String documentRootKey = UUID.randomUUID().toString();;
        final Map<String, Object> jsonMap = Map.of("document_root_key", documentRootKey);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getDocumentRootKey(), equalTo(documentRootKey));
    }

    @Test
    void getMaxRetries_returns_default_when_maxRetries_is_null() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getMaxRetries(), equalTo(DEFAULT_RETRIES));
    }

    @Test
    void getMaxRetries_returns_value_from_deserialized_JSON() {
        final int maxRetries = 3;
        final Map<String, Object> jsonMap = Map.of("max_retries", maxRetries);
        final PersonalizeSinkConfiguration objectUnderTest = objectMapper.convertValue(jsonMap, PersonalizeSinkConfiguration.class);
        assertThat(objectUnderTest.getMaxRetries(), equalTo(maxRetries));
    }
}
