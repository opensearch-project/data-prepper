package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;
import software.amazon.awssdk.services.opensearchserverless.model.CreateSecurityPolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.model.GetSecurityPolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.model.GetSecurityPolicyResponse;
import software.amazon.awssdk.services.opensearchserverless.model.ResourceNotFoundException;
import software.amazon.awssdk.services.opensearchserverless.model.SecurityPolicyDetail;
import software.amazon.awssdk.services.opensearchserverless.model.UpdateSecurityPolicyRequest;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.COLLECTION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.CREATED_BY_DATA_PREPPER;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.DESCRIPTION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.RESOURCE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.RESOURCE_TYPE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.RULES;
import static org.opensearch.dataprepper.plugins.sink.opensearch.ServerlessNetworkPolicyUpdater.SOURCE_VPCES;

public class ServerlessNetworkPolicyUpdaterTest {

    @Mock
    private OpenSearchServerlessClient client;

    @InjectMocks
    private ServerlessNetworkPolicyUpdater updater;


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUpdateNetworkPolicyNoExistingPolicy() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mock client to throw ResourceNotFoundException
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenThrow(ResourceNotFoundException.class);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        final ArgumentCaptor<CreateSecurityPolicyRequest> argumentCaptor = ArgumentCaptor.forClass(CreateSecurityPolicyRequest.class);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client).createSecurityPolicy(argumentCaptor.capture());

        final Document expectedPolicy = Document.fromList(List.of(
            Document.mapBuilder()
                .putString(DESCRIPTION, CREATED_BY_DATA_PREPPER)
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .build()
        ));

        final CreateSecurityPolicyRequest actualRequest = argumentCaptor.getValue();
        final String actualPolicy = actualRequest.policy();
        assertThat(actualPolicy, equalTo(expectedPolicy.toString()));
    }


    @Test
    public void testUpdateNetworkPolicyNoPolicyDetail() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        final GetSecurityPolicyResponse mockedResponse = GetSecurityPolicyResponse.builder().build();

        // Mocking client behavior
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(mockedResponse);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        final ArgumentCaptor<CreateSecurityPolicyRequest> argumentCaptor = ArgumentCaptor.forClass(CreateSecurityPolicyRequest.class);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client).createSecurityPolicy(argumentCaptor.capture());

        final Document expectedPolicy = Document.fromList(List.of(
            Document.mapBuilder()
                .putString(DESCRIPTION, CREATED_BY_DATA_PREPPER)
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .build()
        ));

        final CreateSecurityPolicyRequest actualRequest = argumentCaptor.getValue();
        final String actualPolicy = actualRequest.policy();
        assertThat(actualPolicy, equalTo(expectedPolicy.toString()));
    }

    @Test
    public void testUpdateNetworkPolicyExistingAcceptablePolicyBothConditionsTrueFullMatch() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));
    }

    @Test
    public void testUpdateNetworkPolicyExistingAcceptablePolicyBothConditionsTrueWildcardMatch() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/*")))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));
    }

    @Test
    public void testUpdateNetworkPolicyExistingUnacceptablePolicyOnlyVpceMatches() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/differentCollection")))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        final ArgumentCaptor<UpdateSecurityPolicyRequest> argumentCaptor = ArgumentCaptor.forClass(UpdateSecurityPolicyRequest.class);

        verify(client).updateSecurityPolicy(argumentCaptor.capture());
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));

        final Document expectedPolicy = Document.fromList(List.of(
            policy.asList().get(0),
            Document.mapBuilder()
                .putString(DESCRIPTION, CREATED_BY_DATA_PREPPER)
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .build()
        ));

        final UpdateSecurityPolicyRequest actualRequest = argumentCaptor.getValue();
        final String actualPolicy = actualRequest.policy();

        assertThat(actualPolicy, equalTo(expectedPolicy.toString()));
    }

    @Test
    public void testUpdateNetworkPolicyExistingUnacceptablePolicyOnlyCollectionMatches() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(SOURCE_VPCES, List.of(Document.fromString("vpce5678")))
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        final ArgumentCaptor<UpdateSecurityPolicyRequest> argumentCaptor = ArgumentCaptor.forClass(UpdateSecurityPolicyRequest.class);

        verify(client).updateSecurityPolicy(argumentCaptor.capture());
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));

        final Document expectedPolicy = Document.fromList(List.of(
            policy.asList().get(0),
            Document.mapBuilder()
                .putString(DESCRIPTION, CREATED_BY_DATA_PREPPER)
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .build()
        ));

        final UpdateSecurityPolicyRequest actualRequest = argumentCaptor.getValue();
        final String actualPolicy = actualRequest.policy();

        assertThat(actualPolicy, equalTo(expectedPolicy.toString()));
    }

    @Test
    public void testUpdateNetworkPolicyExistingUnacceptablePolicyBothConditionsFalse() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(SOURCE_VPCES, List.of(Document.fromString("vpce5678")))
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/differentCollection")))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        final ArgumentCaptor<UpdateSecurityPolicyRequest> argumentCaptor = ArgumentCaptor.forClass(UpdateSecurityPolicyRequest.class);

        verify(client).updateSecurityPolicy(argumentCaptor.capture());
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));

        final Document expectedPolicy = Document.fromList(List.of(
            policy.asList().get(0),
            Document.mapBuilder()
                .putString(DESCRIPTION, CREATED_BY_DATA_PREPPER)
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
                .build()
        ));

        final UpdateSecurityPolicyRequest actualRequest = argumentCaptor.getValue();
        final String actualPolicy = actualRequest.policy();

        assertThat(actualPolicy, equalTo(expectedPolicy.toString()));
    }

    @Test
    public void testUpdateNetworkPolicyGetExceptionScenario() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mock client to throw a generic exception
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenThrow(new RuntimeException("Test Exception"));

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));
    }

    @Test
    public void testUpdateNetworkPolicyCreateExceptionScenario() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenThrow(ResourceNotFoundException.class);

        when(client.createSecurityPolicy(any(CreateSecurityPolicyRequest.class)))
            .thenThrow(new RuntimeException("Test Exception"));

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        verify(client, never()).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));
    }

    @Test
    public void testUpdateNetworkPolicyUpdateExceptionScenario() {
        final String networkPolicyName = UUID.randomUUID().toString();
        final String collectionName = UUID.randomUUID().toString();
        final String vpceId = UUID.randomUUID().toString();

        // Mocking an existing policy with acceptable conditions
        final Document policy = Document.fromList(List.of(
            Document.mapBuilder()
                .putList(RULES, List.of(
                    Document.mapBuilder()
                        .putString(RESOURCE_TYPE, COLLECTION)
                        .putList(RESOURCE, List.of(Document.fromString(COLLECTION + "/" + collectionName)))
                        .build()))
                .build()
        ));

        final GetSecurityPolicyResponse response = mock(GetSecurityPolicyResponse.class);
        when(response.securityPolicyDetail()).thenReturn(SecurityPolicyDetail.builder().policy(policy).build());
        when(client.getSecurityPolicy(any(GetSecurityPolicyRequest.class))).thenReturn(response);

        when(client.updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class)))
            .thenThrow(new RuntimeException("Test exception"));

        updater.updateNetworkPolicy(networkPolicyName, collectionName, vpceId);

        verify(client).updateSecurityPolicy(any(UpdateSecurityPolicyRequest.class));
        verify(client, never()).createSecurityPolicy(any(CreateSecurityPolicyRequest.class));
    }
}
