/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class RssItemMapperTest {

    private final RssItemMapper mapper = new RssItemMapper();

    private Item item() {
        return RssTestFixtures.item("t", "https://example.com/a", "d",
                "Mon, 01 Jan 2024 00:00:00 GMT", "guid-1");
    }

    @Test
    void maps_all_fields_to_event() {
        final Record<Event> record = mapper.map(item(), "https://example.com/feed", "tech");
        final Event event = record.getData();
        assertThat(event.get("title", String.class), equalTo("t"));
        assertThat(event.get("link", String.class), equalTo("https://example.com/a"));
        assertThat(event.get("description", String.class), equalTo("d"));
        assertThat(event.get("pub_date", String.class), equalTo("Mon, 01 Jan 2024 00:00:00 GMT"));
        assertThat(event.get("guid", String.class), equalTo("guid-1"));
    }

    @Test
    void guid_falls_back_to_link_when_absent() {
        final Item item = RssTestFixtures.item(null, "https://example.com/b", null, null, null);
        final Record<Event> record = mapper.map(item, "https://example.com/feed", null);
        assertThat(record.getData().get("guid", String.class), equalTo("https://example.com/b"));
    }

    @Test
    void puts_feed_name_and_redacted_feed_url_in_body() {
        final Record<Event> record = mapper.map(item(),
                "https://example.com/feed?token=secret", "tech");
        final Event event = record.getData();
        assertThat(event.get(RssItemMapper.FEED_NAME_ATTRIBUTE, String.class),
                equalTo("tech"));
        assertThat(event.get(RssItemMapper.FEED_URL_ATTRIBUTE, String.class),
                equalTo("https://example.com/feed?<redacted>"));
    }

    @Test
    void attaches_channel_metadata_when_present() {
        // RssTestFixtures builds a channel with title "test" and link "https://example.com",
        // plus the language and category injected here.
        final Item item = RssTestFixtures.items(
                "<language>en-us</language><category>sports</category>"
                        + "<item><guid>g</guid><link>https://example.com/g</link></item>").get(0);
        final Event event = mapper.map(item, "https://example.com/feed", "tech").getData();
        assertThat(event.getMetadata().getAttribute(RssItemMapper.FEED_TITLE_ATTRIBUTE),
                equalTo("test"));
        assertThat(event.getMetadata().getAttribute(RssItemMapper.FEED_LINK_ATTRIBUTE),
                equalTo("https://example.com"));
        assertThat(event.getMetadata().getAttribute(RssItemMapper.FEED_LANGUAGE_ATTRIBUTE),
                equalTo("en-us"));
        assertThat((List<String>) event.getMetadata().getAttribute(RssItemMapper.FEED_CATEGORIES_ATTRIBUTE),
                hasItem("sports"));
    }

    @Test
    void omits_channel_metadata_when_absent() {
        // No <language> or <category> in the channel -> those attributes are not attached.
        final Item item = RssTestFixtures.item("t", "https://example.com/a", "d", null, "g");
        final Event event = mapper.map(item, "https://example.com/feed", "tech").getData();
        assertThat(event.getMetadata().getAttribute(RssItemMapper.FEED_LANGUAGE_ATTRIBUTE),
                nullValue());
        assertThat(event.getMetadata().getAttribute(RssItemMapper.FEED_CATEGORIES_ATTRIBUTE),
                nullValue());
    }

    @Test
    void dedupKey_prefers_guid_then_link() {
        final Item withGuid = RssTestFixtures.item(null, "l", null, null, "g");
        final Item linkOnly = RssTestFixtures.item(null, "l2", null, null, null);
        assertThat(mapper.dedupKey(withGuid), equalTo("g"));
        assertThat(mapper.dedupKey(linkOnly), equalTo("l2"));
    }
}
