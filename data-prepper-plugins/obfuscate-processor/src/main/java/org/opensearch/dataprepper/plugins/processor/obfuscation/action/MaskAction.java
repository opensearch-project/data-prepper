/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.List;
import org.opensearch.dataprepper.model.pattern.Pattern;

@DataPrepperPlugin(name = "mask", pluginType = ObfuscationAction.class, pluginConfigurationType = MaskActionConfig.class)
public class MaskAction implements ObfuscationAction {
    private final String baseReplacement;

    @DataPrepperPluginConstructor
    public MaskAction(final MaskActionConfig config) {
        this.baseReplacement = config.getMaskCharacter().repeat(config.getMaskCharacterLength());
    }

    @Override
    public String obfuscate(String source, List<Pattern> patterns, Record<Event> record) {

        if (patterns == null || patterns.size() == 0) {
            // This is to replace the whole field.
            return baseReplacement;
        }
        for (Pattern pattern : patterns) {
            // Simply use the replaceAll function with baseReplacement string.
            source = source.replaceAll(pattern.pattern(), baseReplacement);
        }
        return source;
    }


}
