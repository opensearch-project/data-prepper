package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexTemplatesRequest;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.indices.IndexTemplateMetadata;
import org.opensearch.client.indices.IndexTemplatesExistRequest;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class IndexManager {

    public static final String INDEX_ALIAS_USED_AS_INDEX_ERROR
            = "Invalid alias name [%s], an index exists with the same name as the alias";
    protected RestHighLevelClient restHighLevelClient;
    protected OpenSearchSinkConfiguration openSearchSinkConfiguration;
    private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);

    protected IndexManager(final RestHighLevelClient restHighLevelClient, final OpenSearchSinkConfiguration openSearchSinkConfiguration){
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchSinkConfiguration);
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchSinkConfiguration = openSearchSinkConfiguration;
    }

    public boolean checkISMEnabled() throws IOException {
        final ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        request.includeDefaults(true);
        final ClusterGetSettingsResponse response = restHighLevelClient.cluster().getSettings(request, RequestOptions.DEFAULT);
        final String enabled = response.getSetting(IndexConstants.ISM_ENABLED_SETTING);
        return enabled != null && enabled.equals("true");
    }

    public void checkAndCreateIndexTemplate(final boolean isISMEnabled, final String ismPolicyId) throws IOException {
        final String indexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        final String indexTemplateName = indexAlias + "-index-template";

        // Check existing index template version - only overwrite if version is less than or does not exist
        if (!shouldCreateIndexTemplate(indexTemplateName)) {
            return;
        }

        final PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(indexTemplateName);

        putIndexTemplateRequest.patterns(getIndexPatterns(indexAlias));

        if (isISMEnabled) {
            attachPolicy(openSearchSinkConfiguration.getIndexConfiguration(), ismPolicyId, indexAlias);
        }

        putIndexTemplateRequest.source(openSearchSinkConfiguration.getIndexConfiguration().getIndexTemplate());
        restHighLevelClient.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
    }

    public abstract Optional<String> checkAndCreatePolicy() throws IOException;

    protected Request createPolicyRequestFromFile(final String endPoint, final String fileName) throws IOException {
        final StringBuilder policyJsonBuffer = new StringBuilder();
        try (final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)))) {
            reader.lines().forEach(line -> policyJsonBuffer.append(line).append("\n"));
        }
        final Request request = new Request(HttpMethod.PUT, endPoint);
        request.setJsonEntity(policyJsonBuffer.toString());
        return request;
    }

    protected List<String> getIndexPatterns(final String indexAlias){
        return  Collections.singletonList(indexAlias);
    }

    private Optional<IndexTemplateMetadata> getIndexTemplateMetadata(final String indexTemplateName) throws IOException {
        final IndexTemplatesExistRequest existsRequest = new IndexTemplatesExistRequest(indexTemplateName);
        final boolean exists = restHighLevelClient.indices().existsTemplate(existsRequest, RequestOptions.DEFAULT);
        if (!exists) {
            return Optional.empty();
        }

        final GetIndexTemplatesRequest request = new GetIndexTemplatesRequest(indexTemplateName);
        final GetIndexTemplatesResponse response = restHighLevelClient.indices().getIndexTemplate(request, RequestOptions.DEFAULT);

        if (response.getIndexTemplates().size() == 1) {
            return Optional.of(response.getIndexTemplates().get(0));
        } else {
            throw new RuntimeException(String.format("Found multiple index templates (%s) result when querying for %s",
                    response.getIndexTemplates().size(),
                    indexTemplateName));
        }
    }

    protected boolean shouldCreateIndexTemplate(final String indexTemplateName) throws IOException {
        final Optional<IndexTemplateMetadata> indexTemplateMetadataOptional = getIndexTemplateMetadata(indexTemplateName);
        if (indexTemplateMetadataOptional.isPresent()) {
            final Integer existingTemplateVersion = indexTemplateMetadataOptional.get().version();
            LOG.info("Found version {} for existing index template {}", existingTemplateVersion, indexTemplateName);

            final int newTemplateVersion = (int) openSearchSinkConfiguration.getIndexConfiguration().getIndexTemplate().getOrDefault("version", 0);

            if (existingTemplateVersion != null && existingTemplateVersion >= newTemplateVersion) {
                LOG.info("Index template {} should not be updated, current version {} >= existing version {}",
                        indexTemplateName,
                        existingTemplateVersion,
                        newTemplateVersion);
                return false;

            } else {
                LOG.info("Index template {} should be updated from version {} to version {}",
                        indexTemplateName,
                        existingTemplateVersion,
                        newTemplateVersion);
                return true;
            }
        } else {
            LOG.info("Index template {} does not exist and should be created", indexTemplateName);
            return true;
        }
    }

    protected boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException {
        return restHighLevelClient.indices().exists(new GetIndexRequest(indexAlias), RequestOptions.DEFAULT);
    }

    public void checkAndCreateIndex() throws IOException {
        // Check alias exists
        final String indexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        final boolean indexExists = checkIfIndexExistsOnServer(indexAlias);

        if (!indexExists) {
            final CreateIndexRequest createIndexRequest = getCreateIndexRequest(indexAlias);
            try {
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } catch (OpenSearchException e) {
                if (e.getMessage().contains("resource_already_exists_exception")) {
                    // Do nothing - likely caused by a race condition where the resource was created
                    // by another host before this host's restClient made its request
                } else if (e.getMessage().contains(String.format(INDEX_ALIAS_USED_AS_INDEX_ERROR, indexAlias))) {
                    // TODO: replace IOException with custom data-prepper exception
                    throw new IOException(
                            String.format("An index exists with the same name as the reserved index alias name [%s], please delete or migrate the existing index",
                                    indexAlias));
                } else {
                    throw new IOException(e);
                }
            }
        }
    }

    protected CreateIndexRequest getCreateIndexRequest(final String indexAlias) {
        final String initialIndexName = indexAlias;
        return new CreateIndexRequest(initialIndexName);
    }

    //To suppress warnings on casting index template settings to Map<String, Object>
    @SuppressWarnings("unchecked")
    protected void attachPolicy(
            final IndexConfiguration configuration, final String ismPolicyId, final String rolloverAlias) {
        configuration.getIndexTemplate().putIfAbsent("settings", new HashMap<>());
        if (ismPolicyId != null) {
            ((Map<String, Object>) configuration.getIndexTemplate().get("settings"))
                    .put(IndexConstants.ISM_POLICY_ID_SETTING, ismPolicyId);
        }
        ((Map<String, Object>) configuration.getIndexTemplate().get("settings"))
                .put(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, rolloverAlias);
    }

}
