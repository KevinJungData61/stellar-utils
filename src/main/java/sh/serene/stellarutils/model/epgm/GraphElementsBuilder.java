package sh.serene.stellarutils.model.epgm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

class GraphElementsBuilder<T extends Element> {

    private SparkSession sparkSession;
    private Class<T> type;
    private Dataset<T> elementDataset;
    private List<T> elementBuffer;
    private int maxListSize;

    GraphElementsBuilder(Class<T> type, int maxListSize, SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.type = type;
        this.elementBuffer = new ArrayList<>();
        this.maxListSize = maxListSize;
    }

    void add(T element) {
        elementBuffer.add(element);
        if (elementBuffer.size() >= maxListSize) {
            moveBuffer();
        }
    }

    void addAll(List<T> elementList) {
        for (T element : elementList) {
            add(element);
        }
    }

    private void moveBuffer() {
        if (elementBuffer.isEmpty()) {
            return;
        }
        Dataset<T> elementDatasetNew = sparkSession.createDataset(elementBuffer, Encoders.bean(type));
        elementDataset = (elementDataset == null) ? elementDatasetNew : elementDataset.union(elementDatasetNew);
        elementBuffer = new ArrayList<>();
    }

    Dataset<T> toElements() {
        moveBuffer();
        return elementDataset;
    }

}
