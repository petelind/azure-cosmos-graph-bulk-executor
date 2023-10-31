// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.graph.bulk.impl;

import com.azure.graph.bulk.impl.model.GremlinVertex;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

@TestInstance(Lifecycle.PER_CLASS)
class GremlinVertexDeserializationTest {
    private ObjectMapper mapper;

    @BeforeAll
    void setup() {
        mapper = BulkGremlinObjectMapper.getBulkGremlinObjectMapper();
    }

    @Test
    void testDeserializationOfBasicGremlinVertex() throws JsonProcessingException {
        GremlinVertex vertex = getGremlinVertex();
        String serializedContent = mapper.writeValueAsString(vertex);

        GremlinVertex deserializedVertex = mapper.readValue(serializedContent, GremlinVertex.class);
        assert vertex.equals(deserializedVertex);

    }

    @Test
    void testDeserializationOfBasicGremlinVertexes() throws JsonProcessingException {

        GremlinVertex[] vertexes = new GremlinVertex[10];
        for (int i = 0; i < 10; i++) {
            vertexes[i] = getGremlinVertex();
        }
        // create a list of serialized vertexes, each string for one vertex
        String[] serializedContent = new String[10];
        for (int i = 0; i < 10; i++) {
            serializedContent[i] = mapper.writeValueAsString(vertexes[i]);
        }

        // process each serialized vertex string and deserialize it into a GremlinVertex object
        GremlinVertex[] deserializedVertexes = Arrays.stream(serializedContent).sequential().map(
                serializedVertex -> {
                    try {
                        return mapper.readValue(serializedVertex, GremlinVertex.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        ).toArray(GremlinVertex[]::new);

        assert Arrays.equals(vertexes, deserializedVertexes);







    }

    private GremlinVertex getGremlinVertex() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        Date date = new Date(System.currentTimeMillis());

        GremlinVertex vertex = GremlinVertex.builder()
                .id(UUID.randomUUID().toString())
                .label("Jamie@info.com")
                .properties(new HashMap<>())
                .build();

        vertex.addProperty("pk", "person", true);
        vertex.addProperty("version", "1.0");
        vertex.addProperty("description", "The root schema");
        vertex.addProperty("lastModifiedDate", formatter.format(date));

        return vertex;
    }
}
