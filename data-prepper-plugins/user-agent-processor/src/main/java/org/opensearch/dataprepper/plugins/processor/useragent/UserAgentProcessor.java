/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua_parser.Client;
import ua_parser.Parser;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "user_agent", pluginType = Processor.class, pluginConfigurationType = UserAgentProcessorConfig.class)
public class UserAgentProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(UserAgentProcessor.class);
    private final UserAgentProcessorConfig config;
    private final Parser userAgentParser;
    private final EventKey sourceKey;
    private final EventKey targetKey;

    @DataPrepperPluginConstructor
    public UserAgentProcessor(
            final UserAgentProcessorConfig config,
            final EventKeyFactory eventKeyFactory,
            final PluginMetrics pluginMetrics) {
        super(pluginMetrics);
        this.config = config;
        this.userAgentParser = new CaffeineCachingParser(config.getCacheSize());
        this.sourceKey = config.getSource();
        this.targetKey = eventKeyFactory.createEventKey(config.getTarget(), EventKeyFactory.EventAction.PUT);
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event event = record.getData();

            final String userAgentStr = event.get(sourceKey, String.class);
            if (userAgentStr == null) {
                LOG.error(EVENT, "User agent source field [{}] is missing or null", sourceKey);
                addTagsOnParseFailure(event);
                continue;
            }

            try {
                final Client clientInfo = this.userAgentParser.parse(userAgentStr);

                final Map<String, Object> parsedUserAgent = getParsedUserAgent(clientInfo);
                if (!config.getExcludeOriginal()) {
                    parsedUserAgent.put("original", userAgentStr);
                }
                event.put(targetKey, parsedUserAgent);
            } catch (Exception e) {
                LOG.error(EVENT,
                        "An exception occurred when parsing user agent data from event [{}] with source key [{}]",
                        event, sourceKey, e);
                addTagsOnParseFailure(event);
            }
        }
        return records;
    }


    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }

    private Map<String, Object> getParsedUserAgent(Client clientInfo) {
        String version = getFullVersion(clientInfo.userAgent.major, clientInfo.userAgent.minor, clientInfo.userAgent.patch);

        Map<String, Object> parsedUserAgent = new HashMap<>();
        parsedUserAgent.put("name", clientInfo.userAgent.family);
        parsedUserAgent.put("version", version);
        parsedUserAgent.put("os", getParsedOS(clientInfo));
        parsedUserAgent.put("device", getParsedDevice(clientInfo));

        return parsedUserAgent;
    }

    private String getFullVersion(String major, String minor, String patch) {
        if (Objects.isNull(major)) {
            return "";
        } else if (Objects.isNull(minor)) {
            return major;
        } else if (Objects.isNull(patch)) {
            return major + "." + minor;
        }
        return major + "." + minor + "." + patch;
    }

    private Map<String, String> getParsedOS(Client clientInfo) {
        String version = getFullVersion(clientInfo.os.major, clientInfo.os.minor, clientInfo.os.patch);
        return Map.of(
                "name", clientInfo.os.family,
                "version", version,
                "full", version.isEmpty() ? clientInfo.os.family : (clientInfo.os.family + " " + version)
        );
    }

    private void addTagsOnParseFailure(final Event event) {
        final List<String> tagsOnParseFailure = config.getTagsOnParseFailure();
        if (Objects.nonNull(tagsOnParseFailure) && !tagsOnParseFailure.isEmpty()) {
            event.getMetadata().addTags(tagsOnParseFailure);
        }
    }

    private Map<String, String> getParsedDevice(Client clientInfo) {
        return Map.of("name", clientInfo.device.family);
    }
}
