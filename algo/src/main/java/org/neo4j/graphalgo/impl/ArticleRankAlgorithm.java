package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;

public interface ArticleRankAlgorithm {
    ArticleRankAlgorithm compute(int iterations);

    ArticleRankResult result();

    Algorithm<?> algorithm();

    static ArticleRankAlgorithm of(
            Graph graph,
            double dampingFactor) {
        return of(AllocationTracker.EMPTY, graph, dampingFactor);
    }

    static ArticleRankAlgorithm of(
            AllocationTracker tracker,
            Graph graph,
            double dampingFactor) {
//        if (graph instanceof HugeGraph) {
//            HugeGraph huge = (HugeGraph) graph;
//            return new HugePageRank(tracker, huge, huge, huge, huge, dampingFactor);
//        }
        return new ArticleRank(graph,graph, graph, graph, graph, dampingFactor);
    }

    static ArticleRankAlgorithm of(
            Graph graph,
            double dampingFactor,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        return of(AllocationTracker.EMPTY, graph, dampingFactor, pool, concurrency, batchSize);
    }

    static ArticleRankAlgorithm of(
            AllocationTracker tracker,
            Graph graph,
            double dampingFactor,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
//        if (graph instanceof HugeGraph) {
//            HugeGraph huge = (HugeGraph) graph;
//            return new HugePageRank(
//                    pool,
//                    concurrency,
//                    batchSize,
//                    tracker,
//                    huge,
//                    huge,
//                    huge,
//                    huge,
//                    dampingFactor);
//        }
        return new ArticleRank(
                graph,
                pool,
                concurrency,
                batchSize,
                graph,
                graph,
                graph,
                graph,
                dampingFactor);
    }

}
