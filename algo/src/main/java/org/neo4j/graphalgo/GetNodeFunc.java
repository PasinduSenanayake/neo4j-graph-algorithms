package org.neo4j.graphalgo;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GetNodeFunc {
    @Context
    public GraphDatabaseAPI api;

    @UserFunction("algo.getNodeById")
    @Description("CALL algo.getNodeById(value) - return node for nodeId. null if none exists")
    public Node getNodeById(@Name(value = "nodeId") Number nodeId) {
        try {
            return api.getNodeById(nodeId.longValue());
        } catch (NotFoundException e) {
            return null;
        }
    }

    @UserFunction("algo.getNodesById")
    @Description("CALL algo.getNodesById(values) - return node for nodeIds. empty if none exists")
    public List<Node> getNodesById(@Name(value = "nodeIds") List<Number> nodeIds) {
        return nodeIds.stream().map(this::getNodeById).filter(Objects::nonNull).collect(Collectors.toList());
    }
}
