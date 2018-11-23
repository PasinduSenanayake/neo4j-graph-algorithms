package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.ArticleRankAlgorithm;
import org.neo4j.graphalgo.impl.ArticleRankResult;
import org.neo4j.graphalgo.results.ArticleRankScore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ArticleRankProc {


    @Procedure(value = "algo.articleRank.stream", mode = Mode.READ)
    @Description("CALL algo.articleRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85, weightProperty: null, concurrency:4}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<ArticleRankScore> articleRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        ArticleRankScore.Stats.Builder statsBuilder = new ArticleRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        if(graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        ArticleRankResult scores = evaluate(graph, tracker, terminationFlag, configuration, statsBuilder);

        log.info("ArticleRank: overall memory usage: %s", tracker.getUsageString());

        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return LongStream.range(0, hugeGraph.nodeCount())
                    .mapToObj(i -> {
                        final long nodeId = hugeGraph.toOriginalNodeId(i);
                        return new ArticleRankScore(
                                nodeId,
                                api.getNodeById(nodeId),
                                scores.score(i)
                        );
                    });
        }

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
                .mapToObj(i -> {
                    final long nodeId = graph.toOriginalNodeId(i);
                    return new ArticleRankScore(
                            nodeId,
                            api.getNodeById(nodeId),
                            scores.score(i)
                    );
                });
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            ArticleRankScore.Stats.Builder statsBuilder,
            ProcedureConfiguration configuration) {
        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withoutRelationshipWeights();

        Direction direction = configuration.getDirection(Direction.OUTGOING);
        if (direction == Direction.BOTH) {
            graphLoader.asUndirected(true);
        } else {
            graphLoader.withDirection(direction);
        }


        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }


    private ArticleRankResult evaluate(
            Graph graph,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            ProcedureConfiguration configuration,
            ArticleRankScore.Stats.Builder statsBuilder) {

        double dampingFactor = configuration.get(CONFIG_DAMPING, DEFAULT_DAMPING);
        int iterations = configuration.getIterations(DEFAULT_ITERATIONS);
        final int batchSize = configuration.getBatchSize();
        final int concurrency = configuration.getConcurrency(Pools.getNoThreadsInDefaultPool());
        log.debug("Computing article rank with damping of " + dampingFactor + " and " + iterations + " iterations.");


        List<Node> sourceNodes = configuration.get("sourceNodes", new ArrayList<>());
        LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);

        ArticleRankAlgorithm prAlgo = ArticleRankAlgorithm.of(
                tracker,
                graph,
                dampingFactor,
                Pools.DEFAULT,
                concurrency,
                batchSize);
        Algorithm<?> algo = prAlgo
                .algorithm()
                .withLog(log)
                .withTerminationFlag(terminationFlag);

        statsBuilder.timeEval(() -> prAlgo.compute(iterations));

        statsBuilder
                .withIterations(iterations)
                .withDampingFactor(dampingFactor);

        final ArticleRankResult articleRank = prAlgo.result();
        algo.release();
        graph.release();
        return articleRank;
    }

    private void write(
            Graph graph,
            TerminationFlag terminationFlag,
            ArticleRankResult result,
            ProcedureConfiguration configuration,
            final ArticleRankScore.Stats.Builder statsBuilder) {
        if (configuration.isWriteFlag(true)) {
            log.debug("Writing Article Rank results");
            String propertyName = configuration.getWriteProperty(DEFAULT_SCORE_PROPERTY);
            try (ProgressTimer timer = statsBuilder.timeWrite()) {
                Exporter exporter = Exporter
                        .of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                result.export(propertyName, exporter);
            }
            statsBuilder
                    .withWrite(true)
                    .withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }

}
