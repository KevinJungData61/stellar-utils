package sh.serene.stellarutils.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import sh.serene.stellarutils.entities.Element;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

public class GraphCompare {

    /**
     * helper function to compare two graph collections for equality
     *
     * @param gc1   graph collection 1
     * @param gc2   graph collection 2
     * @return      gc1 == gc2
     */
    public static boolean compareGraphCollections(SparkGraphCollection gc1, SparkGraphCollection gc2) {
        return compareElements(gc1.getVertices(), gc2.getVertices())
                && compareElements(gc1.getEdges(), gc2.getEdges())
                && compareElements(gc1.getGraphHeads(), gc2.getGraphHeads());
    }

    private static <T extends Element> boolean compareElements(Dataset<T> elem1, Dataset<T> elem2) {
        Dataset<Integer> elem1Ints = elem1.map(new ElementHash<>(), Encoders.INT());
        Dataset<Integer> elem2Ints = elem2.map(new ElementHash<>(), Encoders.INT());
        long count = elem1Ints.intersect(elem2Ints).count();
        return (count == elem1Ints.count() && count == elem2Ints.count());
    }
}
