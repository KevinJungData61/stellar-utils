package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.graph.api.StellarGraphMemFactory;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;

import java.util.List;

public class SparkGraphMemFactory implements StellarGraphMemFactory {

    private final SparkSession sparkSession;

    public SparkGraphMemFactory(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * Create memory from list
     *
     * @param elements  element list
     * @param type      element type
     * @return graph memory
     */
    @Override
    public <T> StellarGraphMemory<T> create(List<T> elements, Class<T> type) {
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
    public <T> StellarGraphMemory<T> create(Dataset<T> elements, Class<T> type) {
        return new SparkGraphMemory<>(elements);
    }
}
