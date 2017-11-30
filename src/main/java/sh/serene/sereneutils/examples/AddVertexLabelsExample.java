package sh.serene.sereneutils.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;
import sh.serene.sereneutils.io.json.JSONDataSource;
import sh.serene.sereneutils.model.epgm.VertexCollection;
import sh.serene.sereneutils.model.epgm.ElementId;
import sh.serene.sereneutils.model.epgm.GraphCollection;

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
        GraphCollection epgmGraph = dataSource.getGraphCollection();
        ElementId graphId = epgmGraph.getGraphHeads().first().getId();

        // add property "elite2"
        /*
        PropertyGraph graph = PropertyGraph.fromEPGM(epgmGraph, graphId);
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
        */

        // writing new vertices to collection
        Dataset<VertexCollection> epgmVertexDataset = epgmGraph.getVertices();

        epgmVertexDataset.show(100);

        List<ElementId> graphIdNew = Arrays.asList(ElementId.create());
        Dataset<VertexCollection> newVertexDataset = epgmVertexDataset
                .map((MapFunction<VertexCollection,VertexCollection>) vertex -> {
                    String label = (vertex.getLabel().equals("user")) ? "new user" : vertex.getLabel();
                    return VertexCollection.create(
                            vertex.getId(),
                            vertex.getProperties(),
                            label,
                            graphIdNew);
                }, Encoders.bean(VertexCollection.class));
        newVertexDataset.show(100);
        JavaPairRDD<ElementId,VertexCollection> newVertices = newVertexDataset
                .toJavaRDD()
                .mapToPair((PairFunction<VertexCollection,ElementId,VertexCollection>) v -> (
                        new Tuple2<>(v.getId(), v)));

        JavaRDD<VertexCollection> testVert = newVertexDataset.toJavaRDD().map(v -> v);
        System.out.println(testVert.first().getId());

        // tuple test
        VertexCollection vert = VertexCollection.create(
                ElementId.create(),
                new HashMap<>(),
                "label",
                Arrays.asList(ElementId.create(), ElementId.create()));
        Tuple2<ElementId,VertexCollection> vertexTuple2 = new Tuple2<>(vert.getId(), vert);
        System.out.println(vertexTuple2._2.getId());
        System.out.println(vertexTuple2._2.getLabel());
        Tuple2<ElementId,VertexCollection> vertexTuple21 = vertexTuple2;
        System.out.println(vertexTuple21._2.getId());
        System.out.println(((VertexCollection)vertexTuple2.copy$default$2()).getId());

        System.out.println(newVertices.first()._1.toString());
        System.out.println(newVertices.first()._2().getId().toString());


        JavaRDD<VertexCollection> vertexJavaRDD = epgmVertexDataset
                .toJavaRDD()
                .mapToPair((PairFunction<VertexCollection,ElementId,List<VertexCollection>>) v -> (
                        new Tuple2<>(v.getId(), Arrays.asList(v))
                        ))
                .reduceByKey((Function2<List<VertexCollection>,List<VertexCollection>,List<VertexCollection>>) (v1, v2) -> {
                    List<VertexCollection> v3 = new ArrayList<>(v1);
                    v3.addAll(v2);
                    return v3;
                })
                .fullOuterJoin(newVertices)
                .flatMapValues(
                        (Function<Tuple2<Optional<List<VertexCollection>>,Optional<VertexCollection>>, Iterable<VertexCollection>>)
                                vTuple -> {
                    List<VertexCollection> vList = vTuple._1.orNull();
                    VertexCollection vNew = vTuple._2.orNull();
                    if (vList == null) {
                        return Collections.singletonList(vNew);
                    } else if (vNew == null) {
                        return vList;
                    } else {
                        List<VertexCollection> outList = new ArrayList<>();
                        boolean found = false;
                        for (VertexCollection v : vList) {
                            if (!found && v.equals(vNew)) {
                                outList.add(v.addToGraphs(vNew.getGraphs()));
                                found = true;
                            } else {
                                outList.add(v);
                            }
                        }
                        if (!found) {
                            outList.add(vNew);
                        }
                        return outList;
                    }
                })
                .map((Function<Tuple2<ElementId,VertexCollection>, VertexCollection>) vTuple -> vTuple._2);

        Dataset<VertexCollection> verticesFinal = spark.createDataset(vertexJavaRDD.rdd(), Encoders.bean(VertexCollection.class));
        verticesFinal.show(100);

    }
}
