package sh.serene.sereneutils.io.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.sereneutils.model.epgm.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphCollectionFactory {

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
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }

}
