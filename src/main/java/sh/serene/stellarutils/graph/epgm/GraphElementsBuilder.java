package sh.serene.stellarutils.graph.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.entities.Element;
import sh.serene.stellarutils.entities.ElementId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Graph Elements Builder
 *
 * @param <T>   Element type
 */
class GraphElementsBuilder<T extends Element> implements Serializable {

    private SparkSession sparkSession;
    private Class<T> type;
    private Dataset<T> elementDataset;
    private HashMap<ElementId,T> elementBuffer;
    private int maxListSize;

    /**
     * Creates a new Graph Elements Builder based on given parameters
     *
     * @param type              type of element
     * @param maxListSize       maximum number of elements that fit in memory
     * @param sparkSession      spark session
     */
    GraphElementsBuilder(Class<T> type, int maxListSize, SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.type = type;
        this.elementBuffer = new HashMap<>();
        this.maxListSize = maxListSize;
    }

    /**
     * Add an element
     *
     * @param element   element to add
     */
    void add(T element) {
        if (element == null) {
            throw new NullPointerException("Element was null");
        }
        elementBuffer.put(element.getId(), element);
        if (elementBuffer.size() >= maxListSize) {
            moveBuffer();
        }
    }

    /**
     * Add a list of elements
     *
     * @param elementList   list of elements to add
     */
    void addAll(List<T> elementList) {
        for (T element : elementList) {
            add(element);
        }
    }

    /**
     * Check whether a particular element has been added
     *
     * @param id    element id
     * @return      builder contains element
     */
    boolean contains(ElementId id) {
        if (elementBuffer.containsKey(id)) {
            return true;
        } else {
            return (elementDataset != null &&
                    elementDataset.filter((FilterFunction<T>) element -> element.getId().equals(id)).count() > 0);
        }
    }

    /**
     * Move buffer to dataset
     *
     */
    private void moveBuffer() {
        if (elementBuffer.isEmpty()) {
            return;
        }
        Dataset<T> elementDatasetNew = sparkSession.createDataset(new ArrayList<>(elementBuffer.values()), Encoders.bean(type));
        elementDataset = (elementDataset == null) ? elementDatasetNew : elementDataset.union(elementDatasetNew);
        elementBuffer = new HashMap<>();
    }

    /**
     * Convert to dataset of elements
     *
     * @return      dataset of elements
     */
    Dataset<T> toElements() {
        moveBuffer();
        return elementDataset;
    }

}
