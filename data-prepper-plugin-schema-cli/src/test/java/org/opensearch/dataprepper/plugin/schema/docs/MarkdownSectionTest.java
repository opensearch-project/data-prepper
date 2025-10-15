package org.opensearch.dataprepper.plugin.schema.docs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkdownSectionTest {

    @Test
    void createTitle_createsLevelOneHeading() {
        MarkdownSection section = MarkdownSection.createTitle("Test", "processor");
        assertEquals("Test processor", section.getHeading());
        assertEquals(1, section.getLevel());
        assertEquals(MarkdownSection.SectionType.TITLE, section.getType());
    }

    @Test
    void toMarkdown_withHeading_formatsCorrectly() {
        MarkdownSection section = new MarkdownSection(
            "Configuration",
            "Test content",
            1,
            MarkdownSection.SectionType.CONFIGURATION
        );

        String markdown = section.toMarkdown();
        assertEquals("## Configuration\n\nTest content\n", markdown);
    }

    @Test
    void toMarkdown_withoutHeading_onlyShowsContent() {
        MarkdownSection section = new MarkdownSection(
            null,
            "Test content",
            1,
            MarkdownSection.SectionType.CUSTOM
        );

        String markdown = section.toMarkdown();
        assertEquals("Test content\n", markdown);
    }

    @Test
    void fromHeading_detectsMetricSections() {
        assertEquals(MarkdownSection.SectionType.METRICS,
            MarkdownSection.SectionType.fromHeading("Metrics"));
        assertEquals(MarkdownSection.SectionType.METRICS,
            MarkdownSection.SectionType.fromHeading("Counter"));
        assertEquals(MarkdownSection.SectionType.METRICS,
            MarkdownSection.SectionType.fromHeading("Timer"));
        assertEquals(MarkdownSection.SectionType.METRICS,
            MarkdownSection.SectionType.fromHeading("Other Metrics"));
    }

    @Test
    void fromHeading_detectsStandardSections() {
        assertEquals(MarkdownSection.SectionType.TITLE,
            MarkdownSection.SectionType.fromHeading("Title"));
        assertEquals(MarkdownSection.SectionType.CONFIGURATION,
            MarkdownSection.SectionType.fromHeading("Configuration"));
        assertEquals(MarkdownSection.SectionType.EXAMPLES,
            MarkdownSection.SectionType.fromHeading("Examples"));
    }

    @Test
    void fromHeading_withUnknownSection_returnsCustom() {
        assertEquals(MarkdownSection.SectionType.CUSTOM,
            MarkdownSection.SectionType.fromHeading("Unknown Section"));
    }

    @Test
    void appendContent_addsNewContent() {
        MarkdownSection section = new MarkdownSection(
            "Test",
            "Initial content",
            1,
            MarkdownSection.SectionType.CUSTOM
        );

        section.appendContent("\nMore content");
        assertEquals("Initial content\nMore content", section.getContent());
    }

    @Test
    void appendContent_withNullInitialContent_setsContent() {
        MarkdownSection section = new MarkdownSection(
            "Test",
            null,
            1,
            MarkdownSection.SectionType.CUSTOM
        );

        section.appendContent("New content");
        assertEquals("New content", section.getContent());
    }
}