package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.graph.api.StellarEdgeMemory;

public class SparkEdgeMemory extends SparkGraphMemory<Edge> implements StellarEdgeMemory {

    public SparkEdgeMemory(Dataset<Edge> edgeDataset) {
        super(edgeDataset);
    }

}
