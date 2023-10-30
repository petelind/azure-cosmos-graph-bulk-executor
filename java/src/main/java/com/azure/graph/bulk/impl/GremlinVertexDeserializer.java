package com.azure.graph.bulk.impl;

import com.azure.graph.bulk.impl.model.GremlinPartitionKey;
import com.azure.graph.bulk.impl.model.GremlinVertex;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GremlinVertexDeserializer extends JsonDeserializer<GremlinVertex> {

    @Override
    public GremlinVertex deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        // Begin deserializing
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // Collect everything from the node we need to create a Vertex
        String id = node.get(GremlinFieldNames.VERTEX_ID).asText();
        String label = node.get(GremlinFieldNames.VERTEX_LABEL).asText();

        String partitionKeyName = "pk"; // TODO: add to GremlinFieldNames
        String partitionKeyValue = node.get(partitionKeyName).asText();
        GremlinPartitionKey partitionKey = GremlinPartitionKey.builder()
                .fieldName(partitionKeyName)
                .value(partitionKeyValue)
                .build();

        // now lets process the properties - we need to get the value from the nested array we receive...
        Map<String, Object> properties = new HashMap<>();
        node.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode propertyNode = entry.getValue();
            if (propertyNode.isArray() && propertyNode.size() > 0 && propertyNode.get(0).has(GremlinFieldNames.PROPERTY_VALUE)) {
                // Because everything turns into the string "value" in the Cosmos Gremlin API
                Object value = propertyNode.get(0).get(GremlinFieldNames.PROPERTY_VALUE).asText();
                properties.put(key, value);
            }
        });

        // and now lets just turn it into a vertex...
        GremlinVertex vertex = GremlinVertex.builder()
                .id(id)
                .label(label)
                .partitionKey(partitionKey)
                .properties(properties)
                .build();

        return vertex;
    }
}
