package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphdb.Direction;

import java.util.*;
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

    private int adjustBatchSize(int batchSize) {
        // multiply batchsize by 8 as a very rough estimate of an average
        // degree of 8 for nodes, so that every partition has approx
        // batchSize nodes.
        batchSize <<= 3;
        return batchSize > 0 ? batchSize : Integer.MAX_VALUE;
    }

    private List<Partition> partitionGraph(
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            Degrees degrees) {
        int nodeCount = Math.toIntExact(idMapping.nodeCount());
        PrimitiveIntIterator nodes = nodeIterator.nodeIterator();
        List<Partition> partitions = new ArrayList<>();
        int start = 0;
        while (nodes.hasNext()) {
            Partition partition = new Partition(
                    nodeCount,
                    nodes,
                    degrees,
                    start,
                    batchSize);
            partitions.add(partition);
            start += partition.nodeCount;
        }
        return partitions;
    }

    private List<Partition> createSinglePartition(
            IdMapping idMapping,
            Degrees degrees) {
        return Collections.singletonList(
                new Partition(
                        Math.toIntExact(idMapping.nodeCount()),
                        null,
                        degrees,
                        0,
                        -1
                )
        );
    }
    private ComputeSteps createComputeSteps(
            int concurrency,
            double dampingFactor,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            List<Partition> partitions,
            ExecutorService pool) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_QUEUE_SIZE;
        }
        final int expectedParallelism = Math.min(
                concurrency,
                partitions.size());
        List<ComputeStep> computeSteps = new ArrayList<>(expectedParallelism);
        IntArrayList starts = new IntArrayList(expectedParallelism);
        IntArrayList lengths = new IntArrayList(expectedParallelism);
        int partitionsPerThread = ParallelUtil.threadSize(
                concurrency + 1,
                partitions.size());
        Iterator<Partition> parts = partitions.iterator();

        AverageDegreeCal degreeCentrality = new AverageDegreeCal(graph, pool, concurrency, Direction.OUTGOING);
        degreeCentrality.compute();
        averageDegree = degreeCentrality.average();

        while (parts.hasNext()) {
            Partition partition = parts.next();
            int partitionCount = partition.nodeCount;
            int start = partition.startNode;
            for (int i = 1; i < partitionsPerThread && parts.hasNext(); i++) {
                partition = parts.next();
                partitionCount += partition.nodeCount;
            }

            starts.add(start);
            lengths.add(partitionCount);

            computeSteps.add(new ComputeStep(
                    dampingFactor,
                    relationshipIterator,
                    degrees,
                    partitionCount,
                    start
            ));
        }

        int[] startArray = starts.toArray();
        int[] lengthArray = lengths.toArray();
        for (ComputeStep computeStep : computeSteps) {
            computeStep.setStarts(startArray, lengthArray);
        }
        return new ComputeSteps(concurrency, computeSteps, pool);
    }

    @Override
    public ArticleRank compute(int iterations) {
        assert iterations >= 1;
        computeSteps.run(iterations);
        return this;
    }

    @Override
    public ArticleRankResult result() {
        return computeSteps.getArticleRank();
    }



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

    private static final class ComputeStep implements Runnable, RelationshipConsumer {
        private static final int S_INIT = 0;
        private static final int S_CALC = 1;
        private static final int S_SYNC = 2;

        private int state;

        private int[] starts;
        private int[] lengths;
        private final RelationshipIterator relationshipIterator;
        private final Degrees degrees;

        private final double alpha;
        private final double dampingFactor;

        private double[] articleRank;
        private double[] deltas;
        private int[][] nextScores;
        private int[][] prevScores;

        private final int partitionSize;
        private final int startNode;
        private final int endNode;

        private int srcRankDelta = 0;

        ComputeStep(
                double dampingFactor,
                RelationshipIterator relationshipIterator,
                Degrees degrees,
                int partitionSize,
                int startNode) {
            this.dampingFactor = dampingFactor;
            this.alpha = 1.0 - dampingFactor;
            this.relationshipIterator = relationshipIterator;
            this.degrees = degrees;
            this.partitionSize = partitionSize;
            this.startNode = startNode;
            this.endNode = startNode + partitionSize;
            state = S_INIT;
        }

        void setStarts(int starts[], int[] lengths) {
            this.starts = starts;
            this.lengths = lengths;
        }

        @Override
        public void run() {
            if (state == S_CALC) {
                singleIteration();
                state = S_SYNC;
            } else if (state == S_SYNC) {
                synchronizeScores(combineScores());
                state = S_CALC;
            } else if (state == S_INIT) {
                initialize();
                state = S_CALC;
            }
        }

        private void initialize() {
            this.nextScores = new int[starts.length][];
            Arrays.setAll(nextScores, i -> new int[lengths[i]]);

            double[] partitionRank = new double[partitionSize];
            Arrays.fill(partitionRank, alpha);

            this.articleRank = partitionRank;
            this.deltas = Arrays.copyOf(partitionRank, partitionSize);
        }

        private void singleIteration() {
            int startNode = this.startNode;
            int endNode = this.endNode;
            RelationshipIterator rels = this.relationshipIterator;
            for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
                double delta = deltas[nodeId - startNode];
                if (delta > 0) {
                    int degree = degrees.degree(nodeId, Direction.OUTGOING);
                    if (degree > 0) {
                        srcRankDelta = (int) (100_000 * (delta / (degree+averageDegree)));
                        rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                    }
                }
            }
        }

        @Override
        public boolean accept(
                int sourceNodeId,
                int targetNodeId,
                long relationId) {
            if (srcRankDelta != 0) {
                int idx = binaryLookup(targetNodeId, starts);
                nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
            }
            return true;
        }

        void prepareNextIteration(int[][] prevScores) {
            this.prevScores = prevScores;
        }

        private int[] combineScores() {
            assert prevScores != null;
            assert prevScores.length >= 1;
            int[][] prevScores = this.prevScores;

            int length = prevScores.length;
            int[] allScores = prevScores[0];
            for (int i = 1; i < length; i++) {
                int[] scores = prevScores[i];
                for (int j = 0; j < scores.length; j++) {
                    allScores[j] += scores[j];
                    scores[j] = 0;
                }
            }

            return allScores;
        }

        private void synchronizeScores(int[] allScores) {
            double dampingFactor = this.dampingFactor;
            double[] pageRank = this.articleRank;

            int length = allScores.length;
            for (int i = 0; i < length; i++) {
                int sum = allScores[i];
                double delta = dampingFactor * (sum / 100_000.0);
                pageRank[i] += delta;
                deltas[i] = delta;
                allScores[i] = 0;
            }
        }

    }



    private final class ComputeSteps {
        private final int concurrency;
        private List<ArticleRank.ComputeStep> steps;
        private final ExecutorService pool;
        private int[][][] scores;

        private ComputeSteps(
                int concurrency,
                List<ArticleRank.ComputeStep> steps,
                ExecutorService pool) {
            assert !steps.isEmpty();
            this.concurrency = concurrency;
            this.steps = steps;
            this.pool = pool;
            int stepSize = steps.size();
            scores = new int[stepSize][][];
            Arrays.setAll(scores, i -> new int[stepSize][]);
        }

        ArticleRankResult getArticleRank() {
            ArticleRank.ComputeStep firstStep = steps.get(0);
            if (steps.size() == 1) {
                return new ArticleRank.PrimitiveDoubleArrayResult(firstStep.articleRank);
            }
            double[][] results = new double[steps.size()][];
            Iterator<ArticleRank.ComputeStep> iterator = steps.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                results[i++] = iterator.next().articleRank;
            }
            return new ArticleRank.PartitionedPrimitiveDoubleArrayResult(results, firstStep.starts);
        }

        private void run(int iterations) {
            // initialize data structures
            ParallelUtil.runWithConcurrency(concurrency, steps, pool);
            for (int i = 0; i < iterations && running(); i++) {
                // calculate scores
                ParallelUtil.runWithConcurrency(concurrency, steps, pool);
                synchronizeScores();
                // sync scores
                ParallelUtil.runWithConcurrency(concurrency, steps, pool);
            }
        }

        private void synchronizeScores() {
            int stepSize = steps.size();
            int[][][] scores = this.scores;
            int i;
            for (i = 0; i < stepSize; i++) {
                synchronizeScores(steps.get(i), i, scores);
            }
        }

        private void synchronizeScores(
                ArticleRank.ComputeStep step,
                int idx,
                int[][][] scores) {
            step.prepareNextIteration(scores[idx]);
            int[][] nextScores = step.nextScores;
            for (int j = 0, len = nextScores.length; j < len; j++) {
                scores[j][idx] = nextScores[j];
            }
        }

        void release() {
            steps.clear();
            steps = null;
            scores = null;
        }
    }

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
