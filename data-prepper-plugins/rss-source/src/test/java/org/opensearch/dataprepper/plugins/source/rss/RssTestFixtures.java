/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test helper that builds {@link Item} instances by parsing RSS, mirroring how
 * the library produces items in production, rather than constructing {@code Item}
 * directly (the constructor is deprecated for removal).
 */
final class RssTestFixtures {

    private RssTestFixtures() {
    }

    /**
     * Parses one RSS {@code <item>} with the given fields and returns it. Any
     * field passed as {@code null} is omitted from the XML.
     */
    static Item item(final String title, final String link, final String description,
                     final String pubDate, final String guid) {
        final StringBuilder sb = new StringBuilder();
        appendTag(sb, "title", title);
        appendTag(sb, "link", link);
        appendTag(sb, "description", description);
        appendTag(sb, "pubDate", pubDate);
        appendTag(sb, "guid", guid);
        return items("<item>" + sb + "</item>").get(0);
    }

    /**
     * Parses a feed whose {@code <channel>} body is the given inner XML (one or
     * more {@code <item>} elements) and returns the resulting items in order.
     */
    static List<Item> items(final String channelInnerXml) {
        final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<rss version=\"2.0\"><channel>"
                + "<title>test</title><link>https://example.com</link>"
                + "<description>test feed</description>"
                + channelInnerXml
                + "</channel></rss>";
        final ByteArrayInputStream in =
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return new RssReader().read(in).collect(Collectors.toList());
    }

    private static void appendTag(final StringBuilder sb, final String tag, final String value) {
        if (value != null) {
            sb.append('<').append(tag).append('>')
              .append(value)
              .append("</").append(tag).append('>');
        }
    }
}
