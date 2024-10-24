package org.opensearch.dataprepper.plugins.source.saas.crawler.model;

public enum ContentType {

    PDF("PDF"),
    HTML("HTML"),
    MS_WORD("MS_WORD"),
    PLAIN_TEXT("PLAIN_TEXT"),
    PPT("PPT"),
    RTF("RTF"),
    XML("XML"),
    XSLT("XSLT"),
    MS_EXCEL("MS_EXCEL"),
    CSV("CSV"),
    JSON("JSON"),
    MD("MD");

    private String value;

    private ContentType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    /**
     * Use this in place of valueOf.
     *
     * @param value
     *        real value
     * @return ContentType corresponding to the value
     *
     * @throws IllegalArgumentException
     *         If the specified value does not map to one of the known values in this enum.
     */
    public static ContentType fromValue(String value) {
        if (value == null || "".equals(value)) {
            throw new IllegalArgumentException("Value cannot be null or empty!");
        }

        for (ContentType enumEntry : ContentType.values()) {
            if (enumEntry.toString().equals(value)) {
                return enumEntry;
            }
        }

        throw new IllegalArgumentException("Cannot create enum from " + value + " value!");
    }
}
