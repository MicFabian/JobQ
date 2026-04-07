package com.jobq.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jobq.config.JobQProperties;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class JobPayloadRedactor {

    private final Set<String> redactedFields;

    public JobPayloadRedactor(JobQProperties properties) {
        Set<String> normalized = new HashSet<>();
        for (String field : properties.getDashboard().getRedactedPayloadFields()) {
            if (field != null && !field.isBlank()) {
                normalized.add(field.trim().toLowerCase(Locale.ROOT));
            }
        }
        this.redactedFields = Set.copyOf(normalized);
    }

    public JsonNode redact(JsonNode payload) {
        if (payload == null || payload.isNull() || redactedFields.isEmpty()) {
            return payload;
        }
        return redactNode(payload);
    }

    private JsonNode redactNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return node;
        }
        if (node.isObject()) {
            ObjectNode redacted = JsonNodeFactory.instance.objectNode();
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.fieldNames().forEachRemaining(key -> {
                if (redactedFields.contains(key.toLowerCase(Locale.ROOT))) {
                    redacted.put(key, "[REDACTED]");
                } else {
                    redacted.set(key, redactNode(objectNode.get(key)));
                }
            });
            return redacted;
        }
        if (node.isArray()) {
            ArrayNode redactedArray = JsonNodeFactory.instance.arrayNode();
            for (JsonNode item : node) {
                redactedArray.add(redactNode(item));
            }
            return redactedArray;
        }
        return node.deepCopy();
    }
}
