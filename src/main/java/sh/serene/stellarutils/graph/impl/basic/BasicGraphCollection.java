package sh.serene.stellarutils.graph.impl.basic;

import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.basic.BasicWriter;

public class BasicGraphCollection implements StellarGraphCollection {
    /**
     * Create a writer object
     *
     * @return graph collection writer
     */
    @Override
    public StellarWriter write(String path) {
        return new BasicWriter(this, path);
    }

    /**
     * Get graph at index
     *
     * @param index index
     * @return graph
     */
    @Override
    public StellarGraph get(int index) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Get graph by ID
     *
     * @param graphId graph ID
     * @return graph
     */
    @Override
    public StellarGraph get(ElementId graphId) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Union a graph into this graph collection
     *
     * @param graph graph
     * @return new graph collection
     */
    @Override
    public StellarGraphCollection union(StellarGraph graph) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Union two graph collections
     *
     * @param other other graph collection
     * @return new graph collection
     */
    @Override
    public StellarGraphCollection union(StellarGraphCollection other) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }
}
