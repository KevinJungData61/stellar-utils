package sh.serene.stellarutils.graph.api;

import org.apache.spark.sql.Dataset;

import java.util.List;

public interface StellarGraphMemory<T> {

    /**
     * Get graph memory as list
     *
     * @return  list
     */
    List<T> asList();

    /**
     * Get graph memory as a spark dataset
     *
     * @return  spark dataset
     */
    Dataset<T> asDataset();
}
