package sh.serene.stellarutils.graph.spark;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;

import java.util.List;

/**
 * Spark Graph Memory
 * @param <T>
 */
public class SparkGraphMemory<T> implements StellarGraphMemory<T> {

    private Dataset<T> dataset;

    public SparkGraphMemory(Dataset<T> dataset) {
        this.dataset = dataset;
    }

    /**
     * Get graph memory as list
     *
     * @return list
     */
    @Override
    public List<T> asList() {
        return this.dataset.collectAsList();
    }

    /**
     * Get graph memory as a spark dataset
     *
     * @return spark dataset
     */
    @Override
    public Dataset<T> asDataset() {
        return this.dataset;
    }
}
