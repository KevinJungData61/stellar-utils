package sh.serene.sereneutils.io.json;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.sereneutils.io.common.GraphHeadToIO;
import sh.serene.sereneutils.io.common.IOGraphHead;
import sh.serene.sereneutils.model.epgm.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * JSON Data Sink & Data Source Test
 *
 */
public class JSONDataTest {

    /**
     * temporary directory for reading/writing
     */
    private final String testPath = "tmp.epgm/";

    private SparkSession spark;
    private JSONDataSink jsonDataSink;
    private JSONDataSource jsonDataSource;

    /**
     * Helper class to hash an element to an integer
     * @param <T>   vertex, edge, or graph head
     */
    private static class ElementHash<T extends Element> implements MapFunction<T,Integer> {

        @Override
        public Integer call(T element) throws Exception {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(element.getId().toString());
            stringBuilder.append(element.getLabel());
            if (element instanceof Vertex) {
                ((Vertex)element).getGraphs().forEach(elementId -> stringBuilder.append(elementId.toString()));
            }
            if (element instanceof Edge) {
                Edge e = (Edge) element;
                e.getGraphs().forEach(elementId -> stringBuilder.append(elementId.toString()));
                stringBuilder.append(e.getSrc().toString());
                stringBuilder.append(e.getDst().toString());
            }
            element.getProperties().entrySet().forEach(entry -> {
                stringBuilder.append(entry.getKey());
                stringBuilder.append(entry.getValue().toString());
            });
            return stringBuilder.toString().hashCode();
        }
    }

    /**
     * helper function to compare two graph collections for equality
     *
     * @param gc1   graph collection 1
     * @param gc2   graph collection 2
     * @return      gc1 == gc2
     */
    private boolean compareGraphCollections(GraphCollection gc1, GraphCollection gc2) {
        return compareElements(gc1.getVertices(), gc2.getVertices())
                && compareElements(gc1.getEdges(), gc2.getEdges())
                && compareElements(gc1.getGraphHeads(), gc2.getGraphHeads());
    }

    private <T extends Element> boolean compareElements(Dataset<T> elem1, Dataset<T> elem2) {
        Dataset<Integer> elem1Ints = elem1.map(new ElementHash<>(), Encoders.INT());
        Dataset<Integer> elem2Ints = elem2.map(new ElementHash<>(), Encoders.INT());
        return (elem1Ints.union(elem2Ints).distinct().except(elem1Ints.intersect(elem2Ints)).count() == 0);
    }

    @Before
    public void setUp() {
        spark = SparkSession.builder().appName("JSON Data Test").master("local").getOrCreate();
        jsonDataSink = new JSONDataSink(testPath);
        jsonDataSource = new JSONDataSource(testPath, spark);
    }

    @Test
    public void testSingleGraphNoAttrNoLabel() {
        List<String> graphids = Arrays.asList(ElementId.create().toString());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        List<Vertex> vertices = Arrays.asList(
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids)
        );
        List<Edge> edges = Arrays.asList(
                Edge.create(ElementId.create().toString(),
                        vertices.get(0).getId().toString(),
                        vertices.get(1).getId().toString(),
                        new HashMap<>(),
                        "",
                        graphids),
                Edge.create(ElementId.create().toString(),
                        vertices.get(1).getId().toString(),
                        vertices.get(2).getId().toString(),
                        new HashMap<>(),
                        "",
                        graphids)
        );
        Dataset<Vertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.createDataset(edges, Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        GraphCollection gc = GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
        jsonDataSink.writeGraphCollection(gc);
        GraphCollection gcRead = jsonDataSource.getGraphCollection();

        assertTrue(compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphWithAttrAndLabel() throws Exception {

        List<ElementId> graphids = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        String label1 = "1";
        String label2 = "2";
        Map<String,PropertyValue> prop = new HashMap<>();
        prop.put("string", PropertyValue.create("123"));
        prop.put("boolean", PropertyValue.create(false));
        prop.put("integer", PropertyValue.create(123));
        prop.put("float", PropertyValue.create(1.23));
        List<Vertex> vertices = Arrays.asList(
                Vertex.create(ElementId.create(), prop, label1, graphids),
                Vertex.create(ElementId.create(), prop, label1, graphids),
                Vertex.create(ElementId.create(), prop, label2, graphids)
        );
        List<Edge> edges = Arrays.asList(
                Edge.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        prop,
                        label1,
                        graphids),
                Edge.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        prop,
                        label2,
                        graphids)
        );
        Dataset<Vertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.createDataset(edges, Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        GraphCollection gc = GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
        jsonDataSink.writeGraphCollection(gc);
        GraphCollection gcRead = jsonDataSource.getGraphCollection();

        assertTrue(compareGraphCollections(gc, gcRead));
    }

    @After
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(new File(testPath));
        } catch (IOException e) {
            System.out.println("Unable to delete temporary folder");
        }
    }
}