/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.Field;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;
import org.opensearch.dataprepper.typeconverter.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


@SingleThread
@DataPrepperPlugin(name = "dissect", pluginType = Processor.class, pluginConfigurationType = DissectProcessorConfig.class)
public class DissectProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(DissectProcessor.class);
    private final DissectProcessorConfig dissectConfig;
    private final Map<String, Dissector> dissectorMap = new HashMap<>();
    private final Map<String, TargetType> targetTypeMap;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public DissectProcessor(PluginMetrics pluginMetrics, final DissectProcessorConfig dissectConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.dissectConfig = dissectConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.targetTypeMap = dissectConfig.getTargetTypes();

        Map<String, String> patternsMap = dissectConfig.getMap();
        for (String key : patternsMap.keySet()) {
            Dissector dissector = new Dissector(patternsMap.get(key));
            dissectorMap.put(key, dissector);
        }

        if (dissectConfig.getDissectWhen() != null &&
                (!expressionEvaluator.isValidExpressionStatement(dissectConfig.getDissectWhen()))) {
            throw new InvalidPluginConfigurationException(
                    String.format("dissect_when \"%s\" is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                            dissectConfig.getDissectWhen()));
        }

    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            Event event = record.getData();
            try{
                String dissectWhen = dissectConfig.getDissectWhen();
                if (Objects.nonNull(dissectWhen) && !expressionEvaluator.evaluateConditional(dissectWhen, event)) {
                    continue;
                }
                for (String field: dissectorMap.keySet()){
                    if(event.containsKey(field)){
                        dissectField(event, field);
                    }
                }
            } catch (Exception ex){
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("Error dissecting the event [{}]")
                        .addArgument(record.getData())
                        .setCause(ex)
                        .log();
            }
        }
        return records;
    }

    private void dissectField(Event event, String field){
        Dissector dissector = dissectorMap.get(field);
        String text = event.get(field, String.class);
        if (dissector.dissectText(text)) {
            List<Field> dissectedFields = dissector.getDissectedFields();
            for(Field disectedField: dissectedFields) {
                String dissectFieldName = disectedField.getKey();
                Object dissectFieldValue = convertTargetType(dissectFieldName,disectedField.getValue());
                event.put(disectedField.getKey(), dissectFieldValue);
            }
        }
    }

    private Object convertTargetType(String fieldKey, String fieldValue){
        if(targetTypeMap == null){
            return fieldValue;
        }
        try{
            if(targetTypeMap.containsKey(fieldKey)){
                final TypeConverter<?> converter = targetTypeMap.get(fieldKey).getTargetConverter();
                return converter.convert(fieldValue);
            } else {
                return fieldValue;
            }
        } catch (NumberFormatException ex){
            LOG.error("Unable to convert [{}] to the target type mentioned", fieldKey);
            return fieldValue;
        }
    }



    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }
}
