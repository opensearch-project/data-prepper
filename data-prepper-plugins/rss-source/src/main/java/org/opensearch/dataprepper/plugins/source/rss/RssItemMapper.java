/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Channel;
import com.apptasticsoftware.rssreader.Item;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class RssItemMapper {

    static final String EVENT_TYPE = "rss-item";
    static final String FEED_NAME_ATTRIBUTE = "feed_name";
    static final String FEED_URL_ATTRIBUTE = "feed_url";
    static final String FEED_TITLE_ATTRIBUTE = "feed_title";
    static final String FEED_LINK_ATTRIBUTE = "feed_link";
    static final String FEED_LANGUAGE_ATTRIBUTE = "feed_language";
    static final String FEED_CATEGORIES_ATTRIBUTE = "feed_categories";

    /**
     * Maps an RSS/Atom item to an event carrying the {@code rss-item} schema in
     * the body. {@code feed_name} (the configured feed key) and {@code feed_url}
     * (redacted so query-string tokens are not exposed) are always-present body
     * fields, so they are searchable and usable for routing via {@code ${/feed_name}}.
     * Channel-derived attributes ({@code feed_title}, {@code feed_link},
     * {@code feed_language}, {@code feed_categories}) are attached as event
     * metadata, only when the feed provides them.
     */
    Record<Event> map(final Item item, final String feedUrl, final String feedName) {
        final String link = item.getLink().orElse("");
        final Map<String, Object> data = new LinkedHashMap<>();
        data.put("title", item.getTitle().orElse(""));
        data.put("link", link);
        data.put("description", item.getDescription().orElse(""));
        data.put("pub_date", item.getPubDate().orElse(""));
        data.put("guid", item.getGuid().orElse(link));
        data.put(FEED_NAME_ATTRIBUTE, feedName);
        data.put(FEED_URL_ATTRIBUTE, FeedUrls.redact(feedUrl));

        final Map<String, Object> metadata = new HashMap<>();
        addChannelMetadata(metadata, item.getChannel());

        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(data)
                .withEventMetadataAttributes(metadata)
                .build();
        return new Record<>(event);
    }

    private void addChannelMetadata(final Map<String, Object> metadata, final Channel channel) {
        if (channel == null) {
            return;
        }
        putIfPresent(metadata, FEED_TITLE_ATTRIBUTE, channel.getTitle());
        putIfPresent(metadata, FEED_LINK_ATTRIBUTE, channel.getLink());
        channel.getLanguage().ifPresent(language -> metadata.put(FEED_LANGUAGE_ATTRIBUTE, language));
        final List<String> categories = channel.getCategories();
        if (categories != null && !categories.isEmpty()) {
            metadata.put(FEED_CATEGORIES_ATTRIBUTE, categories);
        }
    }

    private void putIfPresent(final Map<String, Object> metadata, final String key, final String value) {
        if (value != null && !value.isBlank()) {
            metadata.put(key, value);
        }
    }

    /**
     * Returns a stable deduplication key for an item. Prefers {@code guid}, then
     * {@code link}. When an item has neither, falls back to a content hash of
     * title/description/pub_date so that distinct keyless items are not collapsed
     * to a single empty key (which would silently drop all but the first).
     */
    String dedupKey(final Item item) {
        final String guid = item.getGuid().orElse(null);
        if (guid != null && !guid.isBlank()) {
            return guid;
        }
        final String link = item.getLink().orElse(null);
        if (link != null && !link.isBlank()) {
            return link;
        }
        return contentHash(item);
    }

    private String contentHash(final Item item) {
        final String content = item.getTitle().orElse("") + '|'
                + item.getDescription().orElse("") + '|'
                + item.getPubDate().orElse("");
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            final StringBuilder sb = new StringBuilder(hash.length * 2);
            for (final byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (final NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed present on every JVM; treat absence as fatal.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
