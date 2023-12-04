/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse;

import java.util.List;

public interface CommonParseConfig {
    /**
     * The field of the Event that contains the JSON data.
     *
     * @return The name of the source field.
     */
    String getSource();

    /**
     * The destination that the parsed JSON is written to. Defaults to the root of the Event.
     * If the destination field already exists, it will be overwritten.
     *
     * @return The name of the destination field.
     */
    String getDestination();

    /**
     * An optional setting used to specify a JSON Pointer. Pointer points to the JSON key that will be parsed into the destination.
     * There is no pointer by default, meaning that the entirety of source will be parsed. If the target key would overwrite an existing
     * key in the Event then the absolute path of the target key will be placed into destination
     *
     * Note: (should this be configurable/what about double conflicts?)
     * @return String representing JSON Pointer
     */
    String getPointer();

    /**
     * A `List` of `String`s that specifies the tags to be set in the event the processor fails to parse or an unknown
     * exception occurs while parsing. This tag may be used in conditional expressions in other parts of the configuration.
     *
     * @return List of tags to be set on failure
     */
    List<String> getTagsOnFailure();

    /**
     * An optional setting used to specify a conditional expression.
     * If the expression evaluates to true, the processor will parse the source field.
     *
     * @return String representing conditional expression
     */
    String getParseWhen();

    /**
     * An optional setting used to specify whether the destination field should be overwritten if it already exists.
     * Defaults to true.
     */
    boolean getOverwriteIfDestinationExists();
}
