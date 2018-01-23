package sh.serene.stellarutils.graph.impl.local;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.*;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.local.LocalReader;

import java.util.List;
import java.util.Map;

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
     * Get reader object
     *
     * @return reader object
     */
    @Override
    public LocalReader reader() {
        return this.reader;
    }

    /**
     * Get graph buffer
     *
     * @param label      graph label
     * @param properties graph properties
     * @return graph buffer
     */
    @Override
    public LocalGraphBuffer createGraph(String label, Map<String, PropertyValue> properties) {
        return new LocalGraphBuffer(label, properties);
    }
}
