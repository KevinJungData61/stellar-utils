package sh.serene.sereneutils.io.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.sereneutils.model.epgm.*;

import java.util.*;

public class GraphCollectionFactory {

    private static Map<String,PropertyValue> getDummyProperties() {
        Map<String,PropertyValue> properties = new HashMap<>();
        properties.put("string_property", PropertyValue.create("123"));
        properties.put("boolean_property", PropertyValue.create(true));
        properties.put("integer_property", PropertyValue.create(123));
        properties.put("double_property", PropertyValue.create(1.23));
        return properties;
    }

    public static GraphCollection createWithNoAttrNoLabels(SparkSession spark) {
        List<ElementId> graphids = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        List<Vertex> vertices = Arrays.asList(
                Vertex.create(ElementId.create(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create(), new HashMap<>(), "", graphids)
        );
        List<Edge> edges = Arrays.asList(
                Edge.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        new HashMap<>(),
                        "",
                        graphids),
                Edge.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        new HashMap<>(),
                        "",
                        graphids)
        );
        Dataset<Vertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.createDataset(edges, Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

    public static GraphCollection createWithPrimAttr(SparkSession spark) {
        List<ElementId> graphids = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        String label1 = "1";
        String label2 = "2";
        Map<String,PropertyValue> prop = getDummyProperties();
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
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

    public static GraphCollection createSingleGraphNVertices(SparkSession spark, int n) {
        List<ElementId> graphIds = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphIds.get(0), new HashMap<>(), Integer.toString(n) + "_vertices"
        ));
        String label = "1";
        Map<String,PropertyValue> properties = getDummyProperties();
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            vertices.add(Vertex.create(ElementId.create(), properties, label, graphIds));
        }
        List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < n-1; i++) {
            edges.add(
                    Edge.create(ElementId.create(),
                            vertices.get(i).getId(),
                            vertices.get(i+1).getId(),
                            properties,
                            label,
                            graphIds)
            );
        }
        Dataset<Vertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.createDataset(edges, Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

}
