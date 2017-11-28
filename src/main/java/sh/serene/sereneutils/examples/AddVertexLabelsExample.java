package sh.serene.sereneutils.examples;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;
import sh.serene.sereneutils.io.json.JSONDataSource;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;
import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;
import sh.serene.sereneutils.model.graph.OrdinaryGraph;
import sh.serene.sereneutils.model.graph.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AddVertexLabelsExample {

    public static class Label implements Serializable {

        ElementId elementId;
        String value;

        public Label() { }

        public Label(ElementId id, String value) {
            this.elementId = id;
            this.value = value;
        }

        public void setElementId(ElementId id) {
            this.elementId = id;
        }

        public ElementId getElementId() {
            return this.elementId;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils Vertex Labels Example")
                .master("local")
                .getOrCreate();

        JSONDataSource dataSource = new JSONDataSource("small-yelp-hin.epgm/", spark);
        EPGMGraphCollection epgmGraph = dataSource.getGraphCollection();
        ElementId graphId = epgmGraph.getGraphHeads().first().getId();

        // add property "elite2"
        OrdinaryGraph graph = OrdinaryGraph.fromGraphCollectionById(epgmGraph, graphId);
        Dataset<Vertex> vertexDataset = graph.getVertices();
        Dataset<Label> labelDataset = vertexDataset
                .filter((FilterFunction<Vertex>) vertex ->
                    Boolean.valueOf(vertex.getPropertyValue("elite", String.class)))
                .map((MapFunction<Vertex,Label>) vertex -> new Label(vertex.getId(), "elite"),
                        Encoders.bean(Label.class));
        Dataset<Vertex> newVertexDataset = vertexDataset
                .joinWith(labelDataset,
                        vertexDataset
                            .col("id")
                            .equalTo(labelDataset.col("elementId")), "leftouter")
                .map((MapFunction<Tuple2<Vertex,Label>,Vertex>) tuple -> {
                    Vertex vertex = tuple._1;
                    Map<String,PropertyValue> properties = new HashMap<>(vertex.getProperties());
                    if (tuple._2 != null)
                        properties.put("elite2", PropertyValue.create(tuple._2.value.equals("elite")));
                    return Vertex.create(vertex.getId(), properties, vertex.getLabel());
                }, Encoders.bean(Vertex.class));

        // show result
        newVertexDataset
                .map((MapFunction<Vertex,Boolean>) vertex ->
                        vertex.getPropertyValue("elite2", Boolean.class),
                        Encoders.BOOLEAN()).show(5000);
    }
}
