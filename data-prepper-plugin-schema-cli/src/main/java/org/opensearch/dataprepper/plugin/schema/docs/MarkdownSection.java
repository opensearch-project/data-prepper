package org.opensearch.dataprepper.plugin.schema.docs;

/**
 * Represents a section in a markdown document.
 */
public class MarkdownSection {
    // Standard section types
    public static final String TITLE = "Title";
    public static final String CONFIGURATION = "Configuration";
    public static final String EXAMPLES = "Examples";
    public static final String METRICS = "Metrics";

    private final String heading;
    private String content;
    private int order;
    private final SectionType type;
    private final int level; // # = 1, ## = 2, etc.

    public MarkdownSection(final String heading, final String content, final int order, final SectionType type) {
        this(heading, content, order, type, 2); // Default to ## level
    }

    public MarkdownSection(final String heading, final String content, final int order, final SectionType type, final int level) {
        this.heading = heading;
        this.content = content;
        this.order = order;
        this.type = type;
        this.level = level;
    }

    public String getHeading() {
        return heading;
    }

    public String getContent() {
        return content;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public SectionType getType() {
        return type;
    }

    public int getLevel() {
        return level;
    }

    public void appendContent(String additionalContent) {
        if (content == null) {
            content = additionalContent;
        } else {
            content += additionalContent;
        }
    }

    /**
     * Format section as markdown.
     */
    public String toMarkdown() {
        StringBuilder md = new StringBuilder();

        // Add heading with proper level
        if (heading != null) {
            md.append(repeat("#", level))
              .append(" ")
              .append(heading)
              .append("\n\n");
        }

        // Add content if present
        if (content != null && !content.trim().isEmpty()) {
            md.append(content.trim())
              .append("\n");
        }

        return md.toString();
    }

    private String repeat(String str, int times) {
        return new String(new char[times]).replace("\0", str);
    }

    /**
     * Create a new title section.
     */
    public static MarkdownSection createTitle(String pluginName, String pluginType) {
        return new MarkdownSection(
            pluginName + " " + pluginType,
            "", // No content
            0, // Order set later
            SectionType.TITLE,
            1 // # level heading
        );
    }

    /**
     * Enum representing different types of sections with their priority for merging.
     */
    public enum SectionType {
        TITLE(false),
        CONFIGURATION(true),
        EXAMPLES(true),
        METRICS(false),
        CUSTOM_METRICS(false),
        CUSTOM(false);

        private final boolean preferGenerated;

        SectionType(final boolean preferGenerated) {
            this.preferGenerated = preferGenerated;
        }

        public boolean shouldPreferGenerated() {
            return preferGenerated;
        }

        /**
         * Determine section type from heading.
         */
        public static SectionType fromHeading(final String heading) {
            if (heading == null) return CUSTOM;

            switch (heading.toLowerCase()) {
                case "title":
                    return TITLE;
                case "configuration":
                    return CONFIGURATION;
                case "examples":
                    return EXAMPLES;
                case "metrics":
                case "counter":
                case "timer":
                case "other metrics":
                    return METRICS;
                default:
                    if (heading.toLowerCase().contains("metrics")) {
                        return CUSTOM_METRICS;
                    }
                    return CUSTOM;
            }
        }
    }
}