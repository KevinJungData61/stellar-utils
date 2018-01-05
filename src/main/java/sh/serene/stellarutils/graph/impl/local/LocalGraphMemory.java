package sh.serene.stellarutils.graph.impl.local;

import org.apache.spark.sql.Dataset;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;

import java.util.ArrayList;
import java.util.List;

public class LocalGraphMemory<T> implements StellarGraphMemory<T> {

    private List<T> list;

    public LocalGraphMemory(List<T> list) {
        this.list = new ArrayList<>(list);
    }

    /**
     * Get graph memory as list
     *
     * @return list
     */
    @Override
    public List<T> asList() {
        return new ArrayList<>(list);
    }

    /**
     * Get graph memory as a spark dataset
     *
     * @return spark dataset
     */
    @Override
    public Dataset<T> asDataset() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * number of elements
     *
     * @return size
     */
    @Override
    public long size() {
        return list.size();
    }
}
