package sh.serene.stellarutils.graph.api;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.io.api.StellarReader;

import java.util.List;
import java.util.Map;

public interface StellarBackEndFactory {

    /**
     * Create memory from list
     *
     * @param elements  element list
     * @param type      element type
     * @return          graph memory
     */
    <T> StellarGraphMemory<T> createMemory(List<T> elements, Class<T> type);

    /**
     * Create memory from dataset
     *
     * @param elements  element dataset
     * @param type      element type
     * @return          graph memory
     */
    <T> StellarGraphMemory<T> createMemory(Dataset<T> elements, Class<T> type);

    /**
     * Create vertex memory from list
     *
     * @param vertices  vertex list
     * @return          vertex memory
     */
    StellarVertexMemory createVertexMemory(List<Vertex> vertices);

    /**
     * Create vertex memory from dataset
     *
     * @param vertices  vertex dataset
     * @return          vertex memory
     */
    StellarVertexMemory createVertexMemory(Dataset<Vertex> vertices);

    /**
     * Create edge memory from list
     *
     * @param edges     edge list
     * @return          edge memory
     */
    StellarEdgeMemory createEdgeMemory(List<Edge> edges);

    /**
     * Create edge memory from dataset
     *
     * @param edges     edge dataset
     * @return          edge memory
     */
    StellarEdgeMemory createEdgeMemory(Dataset<Edge> edges);

    /**
     * Get reader object
     *
     * @return  reader object
     */
    StellarReader reader();

    /**
     * Get graph buffer
     *
     * @param label         graph label
     * @param properties    graph properties
     * @return  graph buffer
     */
    StellarGraphBuffer createGraph(String label, Map<String,PropertyValue> properties);

}
