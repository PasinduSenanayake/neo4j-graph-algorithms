package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.core.write.Translators;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class ArticleRank {
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
}
