package sh.serene.stellarutils.graph.api;

import org.apache.spark.sql.Dataset;

import java.util.List;

public interface StellarGraphMemFactory {

    /**
     * Create memory from list
     *
     * @param elements  element list
     * @param type      element type
     * @return          graph memory
     */
    <T> StellarGraphMemory<T> create(List<T> elements, Class<T> type);

    /**
     * Create memory from dataset
     *
     * @param elements  element dataset
     * @param type      element type
     * @return          graph memory
     */
    <T> StellarGraphMemory<T> create(Dataset<T> elements, Class<T> type);
}
