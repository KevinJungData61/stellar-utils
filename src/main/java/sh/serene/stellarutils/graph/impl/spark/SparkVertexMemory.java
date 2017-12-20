package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.StellarVertexMemory;

public class SparkVertexMemory extends SparkGraphMemory<Vertex> implements StellarVertexMemory {

    public SparkVertexMemory(Dataset<Vertex> vertexDataset) {
        super(vertexDataset);
    }

}
