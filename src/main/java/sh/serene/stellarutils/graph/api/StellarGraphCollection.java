package sh.serene.stellarutils.graph.api;

import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.io.impl.spark.SparkWriter;

public interface StellarGraphCollection {

    /**
     * Create a writer object
     *
     * @return  graph collection writer
     */
    SparkWriter write();

    /**
     * Get graph at index
     *
     * @param index     index
     * @return          graph
     */
    StellarGraph get(int index);

    /**
     * Get graph by ID
     *
     * @param graphId   graph ID
     * @return          graph
     */
    StellarGraph get(ElementId graphId);

    /**
     * Union a graph into this graph collection
     *
     * @param graph     graph
     * @return          new graph collection
     */
    StellarGraphCollection union(StellarGraph graph);

    /**
     * Union two graph collections
     *
     * @param other     other graph collection
     * @return          new graph collection
     */
    StellarGraphCollection union(StellarGraphCollection other);
}
