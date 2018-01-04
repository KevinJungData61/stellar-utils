package sh.serene.stellarutils.graph.impl.local;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.StellarBackEndFactory;
import sh.serene.stellarutils.graph.api.StellarEdgeMemory;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;
import sh.serene.stellarutils.graph.api.StellarVertexMemory;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.local.LocalReader;

import java.util.List;

public class LocalBackEndFactory implements StellarBackEndFactory {

    LocalReader reader;

    public LocalBackEndFactory() {
        this.reader = new LocalReader();
    }

    /**
     * Create memory from list
     *
     * @param elements element list
     * @param type     element type
     * @return graph memory
     */
    @Override
    public <T> LocalGraphMemory<T> createMemory(List<T> elements, Class<T> type) {
        return new LocalGraphMemory<>(elements);
    }

    /**
     * Create memory from dataset
     *
     * @param elements element dataset
     * @param type     element type
     * @return graph memory
     */
    @Override
    public <T> LocalGraphMemory<T> createMemory(Dataset<T> elements, Class<T> type) {
        return new LocalGraphMemory<>(elements.collectAsList());
    }

    /**
     * Create vertex memory from list
     *
     * @param vertices vertex list
     * @return vertex memory
     */
    @Override
    public StellarVertexMemory createVertexMemory(List<Vertex> vertices) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Create vertex memory from dataset
     *
     * @param vertices vertex dataset
     * @return vertex memory
     */
    @Override
    public StellarVertexMemory createVertexMemory(Dataset<Vertex> vertices) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Create edge memory from list
     *
     * @param edges edge list
     * @return edge memory
     */
    @Override
    public StellarEdgeMemory createEdgeMemory(List<Edge> edges) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Create edge memory from dataset
     *
     * @param edges edge dataset
     * @return edge memory
     */
    @Override
    public StellarEdgeMemory createEdgeMemory(Dataset<Edge> edges) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Get reader object
     *
     * @return reader object
     */
    @Override
    public LocalReader reader() {
        return this.reader;
    }
}
