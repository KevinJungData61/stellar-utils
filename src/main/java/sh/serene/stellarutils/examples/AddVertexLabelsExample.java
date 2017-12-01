package sh.serene.stellarutils.examples;

import org.apache.spark.sql.*;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.model.epgm.PropertyGraph;
import sh.serene.stellarutils.model.epgm.VertexCollection;
import sh.serene.stellarutils.model.epgm.ElementId;
import sh.serene.stellarutils.model.epgm.GraphCollection;

import java.io.Serializable;
import java.util.*;

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
        GraphCollection graphCollection = dataSource.getGraphCollection();
        ElementId graphId = graphCollection.getGraphHeads().first().getId();

        PropertyGraph propertyGraph = PropertyGraph.fromCollection(graphCollection, graphId);
        Dataset<VertexCollection> verticesNew = spark.createDataset(
                Arrays.asList(VertexCollection.create(
                        ElementId.create(),
                        new HashMap<>(),
                        "new one",
                        new ArrayList<>()
                )), Encoders.bean(VertexCollection.class));
        PropertyGraph propertyGraph1 = propertyGraph.addVertices(verticesNew);
        GraphCollection graphCollection1 = propertyGraph1.intoCollection(graphCollection);
        System.out.println(graphCollection1.getVertices().count());
        System.out.println(PropertyGraph.fromCollection(graphCollection1, graphId).getVertices().count());

    }
}
