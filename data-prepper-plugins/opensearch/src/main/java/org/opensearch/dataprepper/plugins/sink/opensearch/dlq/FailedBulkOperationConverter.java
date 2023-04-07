package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.opensearch.dataprepper.plugins.sink.opensearch.ErrorCauseStringCreator.toSingleLineDisplayString;

public class FailedBulkOperationConverter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String pluginName;
    private final String pipelineName;
    private final String pluginId;

    public FailedBulkOperationConverter(final String pipelineName, final String pluginName, final String pluginId) {
        Objects.requireNonNull(pipelineName);
        Objects.requireNonNull(pluginId);
        Objects.requireNonNull(pluginName);
        this.pluginName = pluginName;
        this.pipelineName = pipelineName;
        this.pluginId = pluginId;
    }

    public DlqObject convertToDlqObject(final FailedBulkOperation failedBulkOperation) {

        final BulkOperationWrapper bulkOperationWithHandle = failedBulkOperation.getBulkOperation();
        final BulkOperation bulkOperation = bulkOperationWithHandle.getBulkOperation();
        final BulkResponseItem bulkResponseItem = failedBulkOperation.getBulkResponseItem();

        final Object document = convertDocumentToGenericMap(bulkOperation);

        final FailedDlqData.Builder failedDlqDataBuilder = FailedDlqData.builder()
            .withIndex(bulkOperation.index().index())
            .withIndexId(bulkOperation.index().id())
            .withDocument(document);

        if (bulkResponseItem != null) {
            final String message = toSingleLineDisplayString(bulkResponseItem.error());
            failedDlqDataBuilder.withStatus(bulkResponseItem.status())
                .withMessage(message);
        } else {
            failedDlqDataBuilder.withMessage(failedBulkOperation.getFailure().getMessage());
        }

        return DlqObject.builder()
            .withFailedData(failedDlqDataBuilder.build())
            .withPluginName(pluginName)
            .withPipelineName(pipelineName)
            .withPluginId(pluginId)
            .withEventHandle(bulkOperationWithHandle.getEventHandle())
            .build();
    }

    private Object convertDocumentToGenericMap(final BulkOperation bulkOperation) {
        final SerializedJson document = (SerializedJson) bulkOperation.index().document();
        final byte[] documentBytes = document.getSerializedJson();
        final String jsonString = new String(documentBytes, StandardCharsets.UTF_8);

        try {
            return OBJECT_MAPPER.readValue(jsonString, Object.class);
        } catch(IOException e) {
            return ImmutableMap.of();
        }
    }
}
