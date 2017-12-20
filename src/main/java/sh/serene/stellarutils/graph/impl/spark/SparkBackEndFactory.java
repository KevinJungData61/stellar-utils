package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.StellarBackEndFactory;
import sh.serene.stellarutils.graph.api.StellarEdgeMemory;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;
import sh.serene.stellarutils.graph.api.StellarVertexMemory;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.spark.SparkReader;

import java.util.List;

public class SparkBackEndFactory implements StellarBackEndFactory {

    private final SparkSession sparkSession;
    private final SparkReader reader;

    public SparkBackEndFactory(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.reader = new SparkReader(sparkSession);
    }

    /**
     * Create memory from list
     *
     * @param elements  element list
     * @param type      element type
     * @return graph memory
     */
    @Override
    public <T> StellarGraphMemory<T> createMemory(List<T> elements, Class<T> type) {
        return new SparkGraphMemory<>(sparkSession.createDataset(elements, Encoders.bean(type)));
    }

    /**
     * Create memory from dataset
     *
     * @param elements  element dataset
     * @param type      element type
     * @return graph memory
     */
    @Override
    public <T> StellarGraphMemory<T> createMemory(Dataset<T> elements, Class<T> type) {
        return new SparkGraphMemory<>(elements);
    }

    /**
     * Create vertex memory from list
     *
     * @param vertices vertex list
     * @return vertex memory
     */
    @Override
    public StellarVertexMemory createVertexMemory(List<Vertex> vertices) {
        return new SparkVertexMemory(sparkSession.createDataset(vertices, Encoders.bean(Vertex.class)));
    }

    /**
     * Create vertex memory from dataset
     *
     * @param vertices vertex dataset
     * @return vertex memory
     */
    @Override
    public StellarVertexMemory createVertexMemory(Dataset<Vertex> vertices) {
        return new SparkVertexMemory(vertices);
    }

    /**
     * Create edge memory from list
     *
     * @param edges edge list
     * @return edge memory
     */
    @Override
    public StellarEdgeMemory createEdgeMemory(List<Edge> edges) {
        return new SparkEdgeMemory(sparkSession.createDataset(edges, Encoders.bean(Edge.class)));
    }

    /**
     * Create edge memory from dataset
     *
     * @param edges edge dataset
     * @return edge memory
     */
    @Override
    public StellarEdgeMemory createEdgeMemory(Dataset<Edge> edges) {
        return new SparkEdgeMemory(edges);
    }

    /**
     * Get reader object
     *
     * @return reader object
     */
    @Override
    public StellarReader reader() {
        return this.reader;
    }
}
