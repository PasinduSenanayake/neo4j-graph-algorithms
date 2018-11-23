package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.write.Exporter;

public interface ArticleRankResult {

    double score(int nodeId);

    double score(long nodeId);

    void export(String propertyName, Exporter exporter);
}
