package com.microsoft.graph.bulk.impl.impl;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.implementation.JsonSerializable;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.microsoft.graph.bulk.impl.GraphBulkExecutor;
import com.microsoft.graph.bulk.impl.ObjectToEdge;
import com.microsoft.graph.bulk.impl.ObjectToVertex;
import com.microsoft.graph.bulk.impl.model.GremlinEdge;
import com.microsoft.graph.bulk.impl.model.GremlinVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class CosmosDBSQLBulkExecutor<V, E> implements GraphBulkExecutor<V, E> {
    private final CosmosAsyncContainer container;
    private final ObjectMapper mapper;
    private final boolean allowUpsert;

    public CosmosDBSQLBulkExecutor(CosmosAsyncContainer container,
                                   ObjectMapper mapper, boolean allowUpsert) {
        this.container = container;
        this.mapper = mapper;
        this.allowUpsert = allowUpsert;
    }

    public Flux<CosmosBulkOperationResponse<GraphBulkExecutor<V, E>>> execute(
            Iterable<V> vertices, Iterable<E> edges) {
        if (vertices == null) vertices = new ArrayList<>();
        if (edges == null) edges = new ArrayList<>();

        var vertexOperations = getVertexStream(vertices).map(this::getVertexOperation);
        var edgeOperations = getEdgeStream(edges).map(this::getEdgeOperation);
        var allOperations = Stream.concat(vertexOperations, edgeOperations);

        return container.executeBulkOperations(Flux.fromStream(allOperations));
    }

    private Stream<GremlinVertex> getVertexStream(Iterable<V> vertices) {
        return StreamSupport.stream(vertices.spliterator(), true).map(v -> {
            var vertex = (v instanceof GremlinVertex) ?
                    (GremlinVertex) v :
                    ObjectToVertex.toGremlinVertex(v);
            vertex.validate();
            return vertex;
        });
    }

    @SneakyThrows // Letting JsonProcessingException bubble up
    private CosmosItemOperation getVertexOperation(GremlinVertex vertex) {
        var partitionKey = new PartitionKey(vertex.getPartitionKey().getValue());

        //TODO: Review this to determine if there is a better approach to getting this operation created, one that
        // doesn't have to deserialize the string that is already in the correct structure for the operation
        if (allowUpsert)
            return CosmosBulkOperations.getUpsertItemOperation(
                    new JsonSerializable(mapper.writeValueAsString(vertex)),
                    partitionKey);

        return CosmosBulkOperations.getCreateItemOperation(
                new JsonSerializable(mapper.writeValueAsString(vertex)),
                partitionKey);
    }

    private Stream<GremlinEdge> getEdgeStream(Iterable<E> edges) {
        return StreamSupport.stream(edges.spliterator(), true).map(e -> {
            var edge = e instanceof GremlinEdge ? (GremlinEdge) e : ObjectToEdge.toGremlinEdge(e);
            edge.validate();
            return edge;
        });
    }

    @SneakyThrows // Letting JsonProcessingException bubble up
    private CosmosItemOperation getEdgeOperation(GremlinEdge edge) {
        var partitionKey = new PartitionKey(edge.getPartitionKey().getValue());

        //TODO: Review this to determine if there is a better approach to getting this operation created, one that
        // doesn't have to deserialize the string that is already in the correct structure for the operation
        if (allowUpsert)
            return CosmosBulkOperations.getUpsertItemOperation(
                    new JsonSerializable(mapper.writeValueAsString(edge)),
                    partitionKey);

        return CosmosBulkOperations.getCreateItemOperation(
                new JsonSerializable(mapper.writeValueAsString(edge)),
                partitionKey);
    }
}
