package sh.serene.stellarutils.model.epgm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class PropertyGraphTest {

    private SparkSession spark;
    private GraphCollection graphCollection;
    private ElementId graphId;
    private int size;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession
                .builder()
                .appName("Stellar Utils Property Graph Test")
                .master("local")
                .getOrCreate();
        size = 1000;
        graphCollection = GraphCollectionFactory.createSingleGraphNVertices(spark, size);
        graphId = graphCollection.getGraphHeads().first().getId();
    }

    private Dataset<EdgeCollection> getNewEdges(int n) {
        List<EdgeCollection> edges = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            edges.add(
                    EdgeCollection.create(ElementId.create(),
                            ElementId.create(),
                            ElementId.create(),
                            new HashMap<>(),
                            "new",
                            new ArrayList<>())
            );
        }
        return spark.createDataset(edges, Encoders.bean(EdgeCollection.class));
    }

    private Dataset<VertexCollection> getNewVertices(int n) {
        List<VertexCollection> vertices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            vertices.add(
                    VertexCollection.create(
                            ElementId.create(),
                            new HashMap<>(),
                            "new",
                            new ArrayList<>()
                    )
            );
        }
        return spark.createDataset(vertices, Encoders.bean(VertexCollection.class));
    }

    @Test
    public void addEdges() throws Exception {
        int n = 100;
        Dataset<EdgeCollection> edgesNew = getNewEdges(n);
        PropertyGraph graphNew = PropertyGraph
                .fromCollection(graphCollection, graphId)
                .addEdges(edgesNew);
        GraphCollection graphCollectionNew = graphNew.intoCollection(graphCollection);

        assertEquals(size - 1 + n, graphNew.getEdges().count());
        assertEquals(size - 1 + n, graphCollectionNew.getEdges().count());
        assertEquals(size - 1,
                PropertyGraph.fromCollection(graphCollectionNew, graphId).getEdges().count());
    }

    @Test
    public void addVertices() throws Exception {
        int n = 100;
        Dataset<VertexCollection> verticesNew = getNewVertices(n);
        PropertyGraph graphNew = PropertyGraph
                .fromCollection(graphCollection, graphId)
                .addVertices(verticesNew);
        GraphCollection graphCollectionNew = graphNew.intoCollection(graphCollection);

        assertEquals(size + n, graphNew.getVertices().count());
        assertEquals(size + n, graphCollectionNew.getVertices().count());
        assertEquals(size,
                PropertyGraph.fromCollection(graphCollectionNew, graphId).getVertices().count());
    }

    @Test
    public void getEdgeList() throws Exception {
        PropertyGraph graph = PropertyGraph.fromCollection(graphCollection, graphId);
        Dataset<EdgeCollection> edges = graph.getEdges();
        Dataset<Tuple2<ElementId,ElementId>> edgeList = graph.getEdgeList();

        assertEquals(edges.count(), edgeList.count());

        long count = edges.joinWith(
                edgeList,
                edges.col("src").equalTo(edgeList.col("_1")).and(
                        edges.col("dst").equalTo(edgeList.col("_2"))
                ),
                "fullouter"
        ).count();

        assertEquals(edges.count(), count);

    }

}