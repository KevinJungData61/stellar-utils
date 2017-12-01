package sh.serene.stellarutils.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.model.epgm.ElementId;
import sh.serene.stellarutils.model.epgm.GraphHead;
import sh.serene.stellarutils.model.epgm.PropertyValue;
import sh.serene.stellarutils.model.epgm.*;

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
        List<VertexCollection> vertices = Arrays.asList(
                VertexCollection.create(ElementId.create(), new HashMap<>(), "", graphids),
                VertexCollection.create(ElementId.create(), new HashMap<>(), "", graphids),
                VertexCollection.create(ElementId.create(), new HashMap<>(), "", graphids)
        );
        List<EdgeCollection> edges = Arrays.asList(
                EdgeCollection.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        new HashMap<>(),
                        "",
                        graphids),
                EdgeCollection.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        new HashMap<>(),
                        "",
                        graphids)
        );
        Dataset<VertexCollection> vertexDataset = spark.createDataset(vertices, Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = spark.createDataset(edges, Encoders.bean(EdgeCollection.class));
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
        List<VertexCollection> vertices = Arrays.asList(
                VertexCollection.create(ElementId.create(), prop, label1, graphids),
                VertexCollection.create(ElementId.create(), prop, label1, graphids),
                VertexCollection.create(ElementId.create(), prop, label2, graphids)
        );
        List<EdgeCollection> edges = Arrays.asList(
                EdgeCollection.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        prop,
                        label1,
                        graphids),
                EdgeCollection.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        prop,
                        label2,
                        graphids)
        );
        Dataset<VertexCollection> vertexDataset = spark.createDataset(vertices, Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = spark.createDataset(edges, Encoders.bean(EdgeCollection.class));
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
        List<VertexCollection> vertices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            vertices.add(VertexCollection.create(ElementId.create(), properties, label, graphIds));
        }
        List<EdgeCollection> edges = new ArrayList<>();
        for (int i = 0; i < n-1; i++) {
            edges.add(
                    EdgeCollection.create(ElementId.create(),
                            vertices.get(i).getId(),
                            vertices.get(i+1).getId(),
                            properties,
                            label,
                            graphIds)
            );
        }
        Dataset<VertexCollection> vertexDataset = spark.createDataset(vertices, Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = spark.createDataset(edges, Encoders.bean(EdgeCollection.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

}
