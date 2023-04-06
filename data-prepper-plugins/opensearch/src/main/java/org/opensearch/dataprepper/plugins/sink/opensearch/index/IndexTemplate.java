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
    void setTemplateName(String name);
    void setIndexPatterns(List<String> indexPatterns);
    void putCustomSetting(String name, Object value);
    Optional<Long> getVersion();
}
