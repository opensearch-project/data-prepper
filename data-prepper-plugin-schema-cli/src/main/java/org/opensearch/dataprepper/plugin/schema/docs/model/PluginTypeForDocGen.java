package org.opensearch.dataprepper.plugin.schema.docs.model;

/**
 * Enumeration of different plugin types in Data Prepper.
 */
public enum PluginTypeForDocGen {
    PROCESSOR,
    SOURCE,
    SINK,
    BUFFER,
    EXTENSION;

    @Override
    public String toString() {
        return this.name().toLowerCase() + "s";
    }
}