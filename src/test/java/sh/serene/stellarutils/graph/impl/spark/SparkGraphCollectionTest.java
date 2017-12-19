package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.GraphHead;
import sh.serene.stellarutils.entities.VertexCollection;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;

import java.util.*;

import static org.junit.Assert.*;

public class SparkGraphCollectionTest {

    private SparkSession spark;
    private SparkGraphCollection sparkGraphCollection;
    private int size;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession
                .builder()
                .appName("Stellar Utils Graph Collection Test")
                .master("local")
                .getOrCreate();
        size = 1000;
        sparkGraphCollection = GraphCollectionFactory.createSingleGraphNVertices(spark, size);
    }

    @Test
    public void fromLists() throws Exception {
        List<GraphHead> graphHeads = Collections.singletonList(GraphHead.create(ElementId.create(), new HashMap<>(), ""));
        List<ElementId> graphs = Collections.singletonList(graphHeads.get(0).getId());
        List<VertexCollection> vertices = new ArrayList<>();
        List<EdgeCollection> edges = new ArrayList<>();
        int n = 100;
        vertices.add(VertexCollection.create(ElementId.create(), new HashMap<>(), "", graphs));
        for (int i = 1; i < n; i++) {
            vertices.add(VertexCollection.create(ElementId.create(), new HashMap<>(), "", graphs));
            edges.add(EdgeCollection.create(
                    ElementId.create(),
                    vertices.get(i-1).getId(),
                    vertices.get(i).getId(),
                    new HashMap<>(),
                    "",
                    graphs
            ));
        }
        SparkGraphCollection sparkGraphCollection = SparkGraphCollection.fromLists(graphHeads, vertices, edges);
        assertEquals(1, sparkGraphCollection.getGraphHeads().count());
        assertEquals(graphs.get(0), sparkGraphCollection.getGraphHeads().first().getId());
        assertEquals(n, sparkGraphCollection.getVertices().count());
        assertEquals(n-1, sparkGraphCollection.getEdges().count());
    }

    @Test
    public void joinVertexCollections() throws Exception {
        // get two vertices from graph collection
        // one to be changed, another to be left unchanged
        List<VertexCollection> verticesOriginal = sparkGraphCollection.getVertices().collectAsList();
        VertexCollection vOriginal1 = verticesOriginal.get(0);
        VertexCollection vOriginal2 = verticesOriginal.get(1);

        // original graph id
        ElementId gOriginal = vOriginal1.getGraphs().get(0);

        // new graph id
        ElementId graphId = ElementId.create();

        // leave one vertex unchanged
        VertexCollection vUnchanged = VertexCollection.create(
                vOriginal1.getId(),
                vOriginal1.getProperties(),
                vOriginal1.getLabel(),
                Arrays.asList(gOriginal, graphId)
        );

        // change one vertex
        VertexCollection vChanged = VertexCollection.create(
                vOriginal2.getId(),
                vOriginal2.getProperties(),
                "changed vertex",
                Collections.singletonList(graphId)
        );

        // create a whole new vertex
        VertexCollection vNew = VertexCollection.create(
                ElementId.create(),
                new HashMap<>(),
                "new vertex",
                Collections.singletonList(graphId)
        );

        // create dataset with new vertices
        Dataset<VertexCollection> verticesNew = spark
                .createDataset(
                        Arrays.asList(
                                vUnchanged,
                                vOriginal2,
                                vChanged,
                                vNew
                        ),
                        Encoders.bean(VertexCollection.class)
                );

        // join vertices
        Dataset<VertexCollection> verticesJoined = sparkGraphCollection.joinVertexCollections(verticesNew);

        assertEquals(size + 2, verticesJoined.count());
        assertEquals(3, verticesJoined.filter((FilterFunction<VertexCollection>) v -> (
                v.getGraphs().contains(graphId))).count());
        assertEquals(1, verticesJoined.filter((FilterFunction<VertexCollection>) v ->
                v.getLabel().equals("changed vertex")).count());
        assertEquals(1, verticesJoined.filter((FilterFunction<VertexCollection>) v -> (
                v.getLabel().equals("new vertex"))).count());
        assertEquals(1, verticesJoined.filter((FilterFunction<VertexCollection>) v ->
                v.getGraphs().size() == 2).count());

    }

    @Test
    public void joinEdgeCollections() throws Exception {
        // get two edges from graph collection
        // one to be changed, another to be left unchanged
        List<EdgeCollection> edgesOriginal = sparkGraphCollection.getEdges().collectAsList();
        EdgeCollection eOriginal1 = edgesOriginal.get(0);
        EdgeCollection eOriginal2 = edgesOriginal.get(1);

        // original graph id
        ElementId gOriginal = eOriginal1.getGraphs().get(0);

        // new graph id
        ElementId graphId = ElementId.create();

        // leave one edge unchanged
        EdgeCollection eUnchanged = EdgeCollection.create(
                eOriginal1.getId(),
                eOriginal1.getSrc(),
                eOriginal1.getDst(),
                eOriginal1.getProperties(),
                eOriginal1.getLabel(),
                Arrays.asList(gOriginal, graphId)
        );

        // change one edge
        EdgeCollection eChanged = EdgeCollection.create(
                eOriginal2.getId(),
                eOriginal2.getSrc(),
                eOriginal2.getDst(),
                eOriginal2.getProperties(),
                "changed edge",
                Collections.singletonList(graphId)
        );

        // create a whole new edge
        EdgeCollection eNew = EdgeCollection.create(
                ElementId.create(),
                ElementId.create(),
                ElementId.create(),
                new HashMap<>(),
                "new edge",
                Collections.singletonList(graphId)
        );

        // create dataset with new vertices
        Dataset<EdgeCollection> edgesNew = spark
                .createDataset(
                        Arrays.asList(
                                eUnchanged,
                                eOriginal2,
                                eChanged,
                                eNew
                        ),
                        Encoders.bean(EdgeCollection.class)
                );

        // join vertices
        Dataset<EdgeCollection> edgesJoined = sparkGraphCollection.joinEdgeCollections(edgesNew);

        assertEquals(size + 1, edgesJoined.count());
        assertEquals(3, edgesJoined.filter((FilterFunction<EdgeCollection>) e -> (
                e.getGraphs().contains(graphId))).count());
        assertEquals(1, edgesJoined.filter((FilterFunction<EdgeCollection>) e ->
                e.getLabel().equals("changed edge")).count());
        assertEquals(1, edgesJoined.filter((FilterFunction<EdgeCollection>) e -> (
                e.getLabel().equals("new edge"))).count());
        assertEquals(1, edgesJoined.filter((FilterFunction<EdgeCollection>) e ->
                e.getGraphs().size() == 2).count());
    }

}