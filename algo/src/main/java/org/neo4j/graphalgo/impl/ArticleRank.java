package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.core.write.Translators;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class ArticleRank extends Algorithm<ArticleRank> implements ArticleRankAlgorithm {

    private final ComputeSteps computeSteps;
    private static double averageDegree;
    private Graph graph;
    /**
     * Forces sequential use. If you want parallelism, prefer
     * {@link #ArticleRank(Graph, ExecutorService, int, int, IdMapping, NodeIterator, RelationshipIterator, Degrees, double)}
     */

    ArticleRank(
            Graph graph,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {

        this(   graph,
                null,
                -1,
                ParallelUtil.DEFAULT_BATCH_SIZE,
                idMapping,
                nodeIterator,
                relationshipIterator,
                degrees,
                dampingFactor);



    }

    ArticleRank(
            Graph graph,
            ExecutorService executor,
            int concurrency,
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {
        this.graph = graph;
        List<Partition> partitions;
        if (ParallelUtil.canRunInParallel(executor)) {
            partitions = partitionGraph(
                    adjustBatchSize(batchSize),
                    idMapping,
                    nodeIterator,
                    degrees);
        } else {
            executor = null;
            partitions = createSinglePartition(idMapping, degrees);
        }

        computeSteps = createComputeSteps(
                concurrency,
                dampingFactor,
                relationshipIterator,
                degrees,
                partitions,
                executor);
    }


    private int srcRankDelta;



    @Override
    public Algorithm<?> algorithm() {
        return this;
    }

    //

    private static final class Partition {

        private final int startNode;
        private final int nodeCount;

        Partition(
                int allNodeCount,
                PrimitiveIntIterator nodes,
                Degrees degrees,
                int startNode,
                int batchSize) {

            int nodeCount;
            int partitionSize = 0;
            if (batchSize > 0) {
                nodeCount = 0;
                while (partitionSize < batchSize && nodes.hasNext()) {
                    int nodeId = nodes.next();
                    ++nodeCount;
                    partitionSize += degrees.degree(nodeId, Direction.OUTGOING);
                }
            } else {
                nodeCount = allNodeCount;
            }

            this.startNode = startNode;
            this.nodeCount = nodeCount;
        }
    }


//    void singleIteration() {
//        int startNode = this.startNode;
//        int endNode = this.endNode;
//        RelationshipIterator rels = this.relationshipIterator;
//        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
//            double delta = deltas[nodeId - startNode];
//            if (delta > 0) {
//                int degree = degrees.degree(nodeId, Direction.OUTGOING);
//                if (degree > 0) {
//                    srcRankDelta = (int) (100_000 * (delta / (degree + averageDegree)));
//                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
//                }
//            }
//        }
//    }
//
//    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
//        if (srcRankDelta != 0) {
//            int idx = binaryLookup(targetNodeId, starts);
//            nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
//        }
//        return true;
//    }

    //

    private static final class PartitionedPrimitiveDoubleArrayResult implements ArticleRankResult, PropertyTranslator.OfDouble<double[][]> {
        private final double[][] partitions;
        private final int[] starts;

        private PartitionedPrimitiveDoubleArrayResult(
                double[][] partitions,
                int[] starts) {
            this.partitions = partitions;
            this.starts = starts;
        }

        @Override
        public void export(
                final String propertyName,
                final Exporter exporter) {
            exporter.write(
                    propertyName,
                    partitions,
                    this
            );
        }

        @Override
        public double toDouble(final double[][] data, final long nodeId) {
            int idx = binaryLookup((int) nodeId, starts);
            return data[idx][(int) (nodeId - starts[idx])];
        }

        @Override
        public double score(final int nodeId) {
            int idx = binaryLookup(nodeId, starts);
            return partitions[idx][nodeId - starts[idx]];
        }

        @Override
        public double score(final long nodeId) {
            return toDouble(partitions, nodeId);
        }
    }

    private static final class PrimitiveDoubleArrayResult implements ArticleRankResult {
        private final double[] result;

        private PrimitiveDoubleArrayResult(double[] result) {
            super();
            this.result = result;
        }

        @Override
        public double score(final int nodeId) {
            return result[nodeId];
        }

        @Override
        public double score(final long nodeId) {
            return score((int) nodeId);
        }

        @Override
        public void export(
                final String propertyName,
                final Exporter exporter) {
            exporter.write(propertyName, result, Translators.DOUBLE_ARRAY_TRANSLATOR);
        }
    }

    @Override
    public ArticleRank me() {
        return this;
    }

    @Override
    public ArticleRank release() {
        computeSteps.release();
        return this;
    }
}
