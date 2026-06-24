/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractIndexManager implements IndexManager {

    public static final String INDEX_ALIAS_USED_AS_INDEX_ERROR
            = "Invalid alias name [%s], an index exists with the same name as the alias";
    public static final String INVALID_INDEX_ALIAS_ERROR
            = "invalid_index_name_exception";
    static final Set<Integer> NO_ISM_HTTP_STATUS = Set.of(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_BAD_REQUEST);
    private static final String TIME_PATTERN_STARTING_SYMBOLS = "%{";
    protected RestHighLevelClient restHighLevelClient;
    protected OpenSearchClient openSearchClient;
    protected OpenSearchSinkConfiguration openSearchSinkConfiguration;
    protected ClusterSettingsParser clusterSettingsParser;
    protected IsmPolicyManagementStrategy ismPolicyManagementStrategy;
    private final TemplateStrategy templateStrategy;
    private final String configuredIndexAlias;
    protected String indexPrefix;
    private Boolean isIndexAlias;
    private boolean isIndexAliasChecked;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexManager.class);

    //For matching a string that begins with a "%{" and ends with a "}".
    //For a string like "data-prepper-%{yyyy-MM-dd}", "%{yyyy-MM-dd}" is matched.
    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "%\\{.*?\\}";
    static final Pattern TIME_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);

    //For matching a string enclosed by "%{" and "}".
    //For a string like "data-prepper-%{yyyy-MM}", "yyyy-MM" is matched.
    private static final String TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION  = "%\\{(.*?)\\}";

    private Optional<DateTimeFormatter> indexDateTimeFormatter;
    private static final ZoneId UTC_ZONE_ID = ZoneId.of(TimeZone.getTimeZone("UTC").getID());

    protected AbstractIndexManager(final RestHighLevelClient restHighLevelClient,
                                   final OpenSearchClient openSearchClient,
                                   final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                   final ClusterSettingsParser clusterSettingsParser,
                                   final TemplateStrategy templateStrategy,
                                   String indexAlias)
    {
        this.templateStrategy = templateStrategy;
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchClient);
        checkNotNull(openSearchSinkConfiguration);
        checkNotNull(clusterSettingsParser);
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchClient = openSearchClient;
        this.openSearchSinkConfiguration = openSearchSinkConfiguration;
        this.clusterSettingsParser = clusterSettingsParser;
        if (indexAlias == null) {
            indexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        }
        configuredIndexAlias = indexAlias;
        initializeIndexPrefixAndSuffix(indexAlias);
    }

    public static DateTimeFormatter getDatePatternFormatter(final String indexAlias) {
        final Pattern pattern = Pattern.compile(TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION);
        final Matcher timePatternMatcher = pattern.matcher(indexAlias);
        if (timePatternMatcher.find()) {
            final String timePattern = timePatternMatcher.group(1);
            if (timePatternMatcher.find()) { // check if there is a one more match.
                throw new IllegalArgumentException("An index only allows one date-time pattern.");
            }
            if(timePattern.contains(TIME_PATTERN_STARTING_SYMBOLS)){ //check if it is a nested pattern such as "data-prepper-%{%{yyyy.MM.dd}}"
                throw new IllegalArgumentException("An index doesn't allow nested date-time patterns.");
            }
            validateNoSpecialCharsInTimePattern(timePattern);
            validateTimePatternGranularity(timePattern);
            return DateTimeFormatter.ofPattern(timePattern);
        }
        return null;
    }

    public static String getIndexAliasWithDate(final String indexAlias) {
        final DateTimeFormatter dateFormatter = getDatePatternFormatter(indexAlias);
        final String dateTimeString = (dateFormatter != null) ? dateFormatter.format(getCurrentUtcTime()) : "";
        return TIME_PATTERN.matcher(indexAlias).replaceAll(dateTimeString);
    }

    private void initializeIndexPrefixAndSuffix(final String indexAlias) {
        final DateTimeFormatter dateFormatter = getDatePatternFormatter(indexAlias);
        if (dateFormatter != null) {
            indexDateTimeFormatter = Optional.of(dateFormatter);
        } else {
            indexDateTimeFormatter = Optional.empty();
        }

        // removes date-time pattern along with leading or trailing hyphen
        indexPrefix = indexAlias
                .replaceAll("-" + TIME_PATTERN_REGULAR_EXPRESSION, "")
                .replaceAll(TIME_PATTERN_REGULAR_EXPRESSION + "-", "");
    }

    /*
     * Special characters can cause failures in creating indexes.
     * */
    private static final Set<Character> INVALID_CHARS = ImmutableSet.of('#', '\\', '/', '*', '?', '"', '<', '>', '|', ',', ':');

    private static void validateNoSpecialCharsInTimePattern(final String timePattern) {
        final boolean containsInvalidCharacter = timePattern.chars()
                .mapToObj(c -> (char) c)
                .anyMatch(character -> INVALID_CHARS.contains(character));
        if (containsInvalidCharacter) {
            throw new IllegalArgumentException("Index time pattern contains one or multiple special characters: " + INVALID_CHARS);
        }
    }

    /*
     * Data Prepper doesn't support creating indexes with time patterns that are too granular, e.g. minute, second, millisecond, nanosecond.
     * https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
     * */
    private static final Set<Character> UNSUPPORTED_TIME_GRANULARITY_CHARS = ImmutableSet.of('m', 's', 'S', 'A', 'n', 'N');

    private static void validateTimePatternGranularity(final String timePattern) {
        final boolean containsUnsupportedTimeSymbol = timePattern.chars()
                .mapToObj(c -> (char) c)
                .anyMatch(character -> UNSUPPORTED_TIME_GRANULARITY_CHARS.contains(character));
        if (containsUnsupportedTimeSymbol) {
            throw new IllegalArgumentException("Index time pattern contains time patterns that are less than one hour: "
                    + UNSUPPORTED_TIME_GRANULARITY_CHARS);
        }
    }

    @Override
    public String getIndexName(final String dynamicIndexAlias) throws IOException {
        if (indexDateTimeFormatter.isPresent()) {
            final String formattedTimeString = indexDateTimeFormatter.get()
                    .format(getCurrentUtcTime());
            return TIME_PATTERN.matcher(configuredIndexAlias).replaceAll(formattedTimeString);
        } else {
            return configuredIndexAlias;
        }
    }

    public static ZonedDateTime getCurrentUtcTime() {
        return LocalDateTime.now().atZone(ZoneId.systemDefault()).withZoneSameInstant(UTC_ZONE_ID);
    }

    @Override
    public Boolean isIndexAlias(final String dynamicIndexAlias) throws IOException {
        if (isIndexAliasChecked == false) {
            try {
                // Try to get the OpenSearch version. This fails on older OpenDistro versions, that do not support
                // `require_alias` as a bulk API parameter. All OpenSearch versions do, as this was introduced in
                // ES 7.10.
                openSearchClient.info();
                ExistsAliasRequest request = new ExistsAliasRequest.Builder().name(dynamicIndexAlias).build();
                BooleanResponse response = openSearchClient.indices().existsAlias(request);
                isIndexAlias = response.value() && checkISMEnabled();
            } catch (RuntimeException ex) {
                isIndexAlias = null;
            } finally {
                isIndexAliasChecked = true;
            }
        }
        return isIndexAlias;
    }

    final boolean checkISMEnabled() throws IOException {
        final GetClusterSettingsRequest request = new GetClusterSettingsRequest.Builder()
                .includeDefaults(true)
                .build();
        try {
            final GetClusterSettingsResponse response = openSearchClient.cluster().getSettings(request);
            final String enabled = getISMEnabled(response);
            return enabled != null && enabled.equals("true");
        } catch (OpenSearchException ex) {
            if (NO_ISM_HTTP_STATUS.contains(ex.status())) {
                return false;
            }
            throw ex;
        }
    }

    private String getISMEnabled(final GetClusterSettingsResponse response) {
        return clusterSettingsParser.getStringValueClusterSetting(response, IndexConstants.ISM_ENABLED_SETTING);
    }

    /**
     * Setups anything required for the index.
     *
     * @throws IOException io exception
     */
    @Override
    public void setupIndex() throws IOException {
        checkAndCreateIndexTemplate();
        checkAndCreateIndex();
    }

    private void checkAndCreateIndexTemplate() throws IOException {
        final boolean isISMEnabled = checkISMEnabled();
        final Optional<String> policyIdOptional = isISMEnabled ?
                ismPolicyManagementStrategy.checkAndCreatePolicy(configuredIndexAlias) :
                Optional.empty();
        if (!openSearchSinkConfiguration.getIndexConfiguration().getIndexTemplate().isEmpty()) {
            checkAndCreateIndexTemplate(isISMEnabled, policyIdOptional.orElse(null));
        }
    }

    final void checkAndCreateIndexTemplate(final boolean isISMEnabled, final String ismPolicyId) throws IOException {
        final String indexTemplateName = indexPrefix  + "-index-template";

        final Map<String, Object> indexTemplateMap = openSearchSinkConfiguration.getIndexConfiguration()
                .getIndexTemplate();
        final IndexTemplate indexTemplate = templateStrategy.createIndexTemplate(indexTemplateMap);

        // Check existing index template version - only overwrite if version is less than or does not exist
        if (!shouldCreateTemplate(indexTemplateName, indexTemplate)) {
            return;
        }

        if (isISMEnabled) {
            attachPolicy(indexTemplate, ismPolicyId, indexPrefix);
        }

        final List<String> indexPatterns = ismPolicyManagementStrategy.getIndexPatterns(configuredIndexAlias);
        indexTemplate.setTemplateName(indexTemplateName);
        indexTemplate.setIndexPatterns(indexPatterns);

        templateStrategy.createTemplate(indexTemplate);
    }

    final Optional<String> checkAndCreatePolicy(final String indexAlias) throws IOException {
        return ismPolicyManagementStrategy.checkAndCreatePolicy(indexAlias);
    }

    public void checkAndCreateIndex() throws IOException {
        // Check if index name exists
        final String indexAlias = getIndexName(null);
        final boolean indexExists = ismPolicyManagementStrategy.checkIfIndexExistsOnServer(indexAlias);

        if (!indexExists) {
            final CreateIndexRequest createIndexRequest = ismPolicyManagementStrategy.getCreateIndexRequest(indexAlias);
            try {
                openSearchClient.indices().create(createIndexRequest);
            } catch (final OpenSearchException e) {
                if (e.getMessage().contains("resource_already_exists_exception")) {
                    // Do nothing - likely caused by a race condition where the resource was created
                    // by another host before this host's restClient made its request
                } else if (e.getMessage().contains(INVALID_INDEX_ALIAS_ERROR)) {
                    throw new InvalidPluginConfigurationException(String.format("Invalid characters in the index name %s", indexAlias));
                } else if (e.getMessage().contains(String.format(INDEX_ALIAS_USED_AS_INDEX_ERROR, indexAlias))) {
                    throw new InvalidPluginConfigurationException(
                            String.format("An index exists with the same name as the reserved index alias name [%s], please delete or migrate the existing index",
                                    indexAlias));
                } else {
                    throw new IOException(e);
                }
            }
        }
    }

    private boolean shouldCreateTemplate(final String templateName, final IndexTemplate indexTemplate) throws IOException {
        final Optional<Long> optionalExistingVersion = templateStrategy.getExistingTemplateVersion(templateName);
        if (optionalExistingVersion.isPresent()) {
            final Long existingTemplateVersion = optionalExistingVersion.get();
            LOG.info("Found version {} for existing index template {}", existingTemplateVersion, templateName);

            final Long newTemplateVersion = indexTemplate.getVersion().orElse(0L);

            if (existingTemplateVersion >= newTemplateVersion) {
                LOG.info("Index template {} should not be updated, current version {} >= existing version {}",
                        templateName,
                        existingTemplateVersion,
                        newTemplateVersion);
                return false;

            } else {
                LOG.info("Index template {} should be updated from version {} to version {}",
                        templateName,
                        existingTemplateVersion,
                        newTemplateVersion);
                return true;
            }
        } else {
            LOG.info("Index template {} does not exist and should be created", templateName);
            return true;
        }
    }

    private void attachPolicy(
            final IndexTemplate indexTemplate, final String ismPolicyId, final String rolloverAlias) {
        if (ismPolicyId != null) {
            indexTemplate.putCustomSetting(IndexConstants.ISM_POLICY_ID_SETTING, ismPolicyId);
        }
        indexTemplate.putCustomSetting(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, rolloverAlias);
	indexTemplate.putCustomSetting(IndexConstants.PLUGINS_ROLLOVER_ALIAS_SETTING, rolloverAlias);
    }

}
