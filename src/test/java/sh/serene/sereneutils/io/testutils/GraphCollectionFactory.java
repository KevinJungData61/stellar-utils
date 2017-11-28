package sh.serene.sereneutils.io.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.GraphHead;
import sh.serene.sereneutils.model.common.PropertyValue;
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

    public static EPGMGraphCollection createWithNoAttrNoLabels(SparkSession spark) {
        List<ElementId> graphids = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        List<EPGMVertex> vertices = Arrays.asList(
                EPGMVertex.create(ElementId.create(), new HashMap<>(), "", graphids),
                EPGMVertex.create(ElementId.create(), new HashMap<>(), "", graphids),
                EPGMVertex.create(ElementId.create(), new HashMap<>(), "", graphids)
        );
        List<EPGMEdge> edges = Arrays.asList(
                EPGMEdge.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        new HashMap<>(),
                        "",
                        graphids),
                EPGMEdge.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        new HashMap<>(),
                        "",
                        graphids)
        );
        Dataset<EPGMVertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(EPGMVertex.class));
        Dataset<EPGMEdge> edgeDataset = spark.createDataset(edges, Encoders.bean(EPGMEdge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return EPGMGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

    public static EPGMGraphCollection createWithPrimAttr(SparkSession spark) {
        List<ElementId> graphids = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        String label1 = "1";
        String label2 = "2";
        Map<String,PropertyValue> prop = getDummyProperties();
        List<EPGMVertex> vertices = Arrays.asList(
                EPGMVertex.create(ElementId.create(), prop, label1, graphids),
                EPGMVertex.create(ElementId.create(), prop, label1, graphids),
                EPGMVertex.create(ElementId.create(), prop, label2, graphids)
        );
        List<EPGMEdge> edges = Arrays.asList(
                EPGMEdge.create(ElementId.create(),
                        vertices.get(0).getId(),
                        vertices.get(1).getId(),
                        prop,
                        label1,
                        graphids),
                EPGMEdge.create(ElementId.create(),
                        vertices.get(1).getId(),
                        vertices.get(2).getId(),
                        prop,
                        label2,
                        graphids)
        );
        Dataset<EPGMVertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(EPGMVertex.class));
        Dataset<EPGMEdge> edgeDataset = spark.createDataset(edges, Encoders.bean(EPGMEdge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return EPGMGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

    public static EPGMGraphCollection createSingleGraphNVertices(SparkSession spark, int n) {
        List<ElementId> graphIds = Arrays.asList(ElementId.create());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphIds.get(0), new HashMap<>(), Integer.toString(n) + "_vertices"
        ));
        String label = "1";
        Map<String,PropertyValue> properties = getDummyProperties();
        List<EPGMVertex> vertices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            vertices.add(EPGMVertex.create(ElementId.create(), properties, label, graphIds));
        }
        List<EPGMEdge> edges = new ArrayList<>();
        for (int i = 0; i < n-1; i++) {
            edges.add(
                    EPGMEdge.create(ElementId.create(),
                            vertices.get(i).getId(),
                            vertices.get(i+1).getId(),
                            properties,
                            label,
                            graphIds)
            );
        }
        Dataset<EPGMVertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(EPGMVertex.class));
        Dataset<EPGMEdge> edgeDataset = spark.createDataset(edges, Encoders.bean(EPGMEdge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        return EPGMGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

}
