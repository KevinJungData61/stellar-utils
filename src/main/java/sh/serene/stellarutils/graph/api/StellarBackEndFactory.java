package sh.serene.stellarutils.graph.api;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.io.api.StellarReader;

import java.util.List;

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
     * Get reader object
     *
     * @return  reader object
     */
    StellarReader reader();

}
