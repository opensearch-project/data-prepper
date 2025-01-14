package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import java.util.HashMap;
import java.util.Map;

public class AttributeHandler {
    public static Map<String, String> collectMetadataAttributes(final Message message, final String queueUrl) {
        final Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put("queueUrl", queueUrl);

        for (Map.Entry<MessageSystemAttributeName, String> entry : message.attributes().entrySet()) {
            String originalKey = entry.getKey().toString();
            String key = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);
            metadataMap.put(key, entry.getValue());
        }

        for (Map.Entry<String, MessageAttributeValue> entry : message.messageAttributes().entrySet()) {
            String originalKey = entry.getKey().toString();
            String key = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);
            metadataMap.put(key, entry.getValue().stringValue());
        }
        return metadataMap;
    }

    public static void applyMetadataAttributes(final Event event, final Map<String, String> attributes) {
        final EventMetadata metadata = event.getMetadata();
        attributes.forEach(metadata::setAttribute);
    }
}
