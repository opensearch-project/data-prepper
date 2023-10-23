package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;
import software.amazon.awssdk.services.opensearchserverless.model.CreateSecurityPolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.model.GetSecurityPolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.model.GetSecurityPolicyResponse;
import software.amazon.awssdk.services.opensearchserverless.model.ResourceNotFoundException;
import software.amazon.awssdk.services.opensearchserverless.model.SecurityPolicyDetail;
import software.amazon.awssdk.services.opensearchserverless.model.SecurityPolicyType;
import software.amazon.awssdk.services.opensearchserverless.model.UpdateSecurityPolicyRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class ServerlessNetworkPolicyUpdater {

    static final String COLLECTION = "collection";
    static final String CREATED_BY_DATA_PREPPER = "Created by Data Prepper";
    static final String DESCRIPTION = "Description";
    static final String RESOURCE = "Resource";
    static final String RESOURCE_TYPE = "ResourceType";
    static final String RULES = "Rules";
    static final String SOURCE_VPCES = "SourceVPCEs";

    private static final Logger LOG = LoggerFactory.getLogger(ServerlessNetworkPolicyUpdater.class);

    private final OpenSearchServerlessClient client;

    public ServerlessNetworkPolicyUpdater(OpenSearchServerlessClient client) {
        this.client = client;
    }

    public void updateNetworkPolicy(
        final String networkPolicyName,
        final String collectionName,
        final String vpceId
    ) {
        try {
            final Document newStatement = createNetworkPolicyStatement(collectionName, vpceId);
            final Optional<SecurityPolicyDetail> maybeNetworkPolicy = getNetworkPolicy(networkPolicyName);

            if (maybeNetworkPolicy.isPresent()) {
                final Document existingPolicy = maybeNetworkPolicy.get().policy();
                final String policyVersion = maybeNetworkPolicy.get().policyVersion();
                final List<Document> existingStatements = existingPolicy.asList();
                if (hasAcceptablePolicy(existingStatements, collectionName, vpceId)) {
                    LOG.info("Policy statement already exists that matches collection and vpce id");
                    return;
                }

                final List<Document> statements = new ArrayList<>(existingStatements);
                statements.add(newStatement);
                final Document newPolicy = Document.fromList(statements);
                updateNetworkPolicy(networkPolicyName, newPolicy, policyVersion);
            } else {
                final Document newPolicy = Document.fromList(List.of(newStatement));
                createNetworkPolicy(networkPolicyName, newPolicy);
            }
        } catch (final Exception e) {
            LOG.error("Failed to create or update network policy", e);
        }
    }
    
    private Optional<SecurityPolicyDetail> getNetworkPolicy(final String networkPolicyName) {
        // Call the GetSecurityPolicy API
        GetSecurityPolicyRequest getRequest = GetSecurityPolicyRequest.builder()
            .name(networkPolicyName)
            .type(SecurityPolicyType.NETWORK)
            .build();

        GetSecurityPolicyResponse response;
        try {
            response = client.getSecurityPolicy(getRequest);
        } catch (final ResourceNotFoundException e) {
            LOG.info("Could not find network policy {}", networkPolicyName);
            return Optional.empty();
        }

        if (response.securityPolicyDetail() == null) {
            LOG.info("Security policy exists but had no detail.");
            return Optional.empty();
        }

        return Optional.of(response.securityPolicyDetail());
    }
    
    private void createNetworkPolicy(final String networkPolicyName, final Document policy) {
        final CreateSecurityPolicyRequest request = CreateSecurityPolicyRequest.builder()
            .name(networkPolicyName)
            .policy(policy.toString())
            .type(SecurityPolicyType.NETWORK)
            .build();

        client.createSecurityPolicy(request);
    }
    
    private void updateNetworkPolicy(final String networkPolicyName, final Document policy, final String policyVersion) {
        final UpdateSecurityPolicyRequest request = UpdateSecurityPolicyRequest.builder()
            .name(networkPolicyName)
            .policy(policy.toString())
            .type(SecurityPolicyType.NETWORK)
            .policyVersion(policyVersion)
            .build();

        client.updateSecurityPolicy(request);
    }
    
    private static Document createNetworkPolicyStatement(final String collectionName, final String vpceId) {
        return Document.mapBuilder()
            .putString(DESCRIPTION, "Created by Data Prepper")
            .putList(RULES, List.of(Document.mapBuilder()
                .putString(RESOURCE_TYPE, COLLECTION)
                .putList(RESOURCE, List.of(Document.fromString(String.format("%s/%s", COLLECTION, collectionName))))
                .build()))
            .putList(SOURCE_VPCES, List.of(Document.fromString(vpceId)))
            .build();
    }
    
    private static boolean hasAcceptablePolicy(final List<Document> statements, final String collectionName, final String vpceId) {
        for (final Document statement : statements) {
            final Map<String, Document> statementFields = statement.asMap();
            if (!statementFields.containsKey(SOURCE_VPCES) || !statementFields.containsKey(RULES)) {
                continue;
            }

            // Check if the statement has the SourceVPCEs field that matches the given vpceId
            boolean hasMatchingVpce = statementFields.get(SOURCE_VPCES).asList().stream()
                .map(Document::asString)
                .anyMatch(vpce -> vpce.equals(vpceId));

            // Check if the statement has the Rules field with the ResourceType set to COLLECTION
            // that matches (or covers) the given collectionName
            boolean hasMatchingCollection = statementFields.get(RULES).asList().stream()
                .filter(rule -> rule.asMap().get(RESOURCE_TYPE).asString().equals(COLLECTION))
                .flatMap(rule -> rule.asMap().get(RESOURCE).asList().stream())
                .map(Document::asString)
                .anyMatch(collectionPattern -> matchesPattern(collectionPattern, String.format("%s/%s", COLLECTION, collectionName)));

            // If both conditions are met, return true
            if (hasMatchingVpce && hasMatchingCollection) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesPattern(String pattern, String value) {
        // Convert wildcard pattern to regex
        String regex = "^" + Pattern.quote(pattern).replace("*", "\\E.*\\Q") + "$";
        return value.matches(regex);
    }

}