/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.List;
import java.util.Optional;

/**
 * Model representation of an Index Template. Allows interacting with a template
 * regardless of the {@link TemplateType} of that template.
 */
public interface IndexTemplate {
    /**
     * Sets the name of the template. This is the name that OpenSearch will use.
     *
     * @param name The template name.
     */
    void setTemplateName(String name);

    /**
     * Sets the index patterns that this template will apply to.
     *
     * @param indexPatterns The index patterns.
     */
    void setIndexPatterns(List<String> indexPatterns);

    /**
     * Puts custom settings to the template. Custom settings here
     * refers to settings which do not have a representation in the
     * base OpenSearch model. So, items like name and version should
     * not be used here.
     *
     * @param name The name of the setting to add to the template
     * @param value The value of the custom setting
     */
    void putCustomSetting(String name, Object value);

    /**
     * Returns the version of the template.
     *
     * @return An {@link Optional} with the version if the template is known to exist. An
     * empty {@link Optional} if this does not have a known version.
     */
    Optional<Long> getVersion();
}
