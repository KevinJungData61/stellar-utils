package sh.serene.stellarutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;

import java.util.*;

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

    private Dataset<Edge> getNewEdges(int n) {
        List<Edge> edges = new ArrayList<>();
        ElementId version = ElementId.create();
        for (int i = 0; i < n; i++) {
            edges.add(
                    Edge.create(ElementId.create(),
                            ElementId.create(),
                            ElementId.create(),
                            new HashMap<>(),
                            "new",
                            version)
            );
        }
        return spark.createDataset(edges, Encoders.bean(Edge.class));
    }

    private Dataset<Vertex> getNewVertices(int n) {
        List<Vertex> vertices = new ArrayList<>();
        ElementId version = ElementId.create();
        for (int i = 0; i < n; i++) {
            vertices.add(
                    Vertex.create(
                            ElementId.create(),
                            new HashMap<>(),
                            "new",
                            version
                    )
            );
        }
        return spark.createDataset(vertices, Encoders.bean(Vertex.class));
    }

    @Test
    public void addEdges() throws Exception {
        int n = 100;
        Dataset<Edge> edgesNew = getNewEdges(n);
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
        Dataset<Vertex> verticesNew = getNewVertices(n);
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
    public void addVertexProperty() throws Exception {
        PropertyGraph graphOrig = PropertyGraph.fromCollection(graphCollection, graphId);
        ElementId vertexId = graphOrig.getVertices().first().getId();
        String key = "newkey";
        PropertyValue prop = PropertyValue.create("value");
        List<Tuple2<ElementId,PropertyValue>> vertexToPropsList = Collections.singletonList(
                new Tuple2<>(vertexId, prop)
        );
        Dataset<Tuple2<ElementId,PropertyValue>> vertexToProps = spark.createDataset(vertexToPropsList, Encoders.tuple(
                Encoders.bean(ElementId.class),
                Encoders.bean(PropertyValue.class)
        ));
        PropertyGraph graphNew = graphOrig.addVertexProperty(key, vertexToProps);
        Vertex vertex = graphNew.getVertices().filter((FilterFunction<Vertex>) v ->
                v.getId().equals(vertexId)).first();
        assertEquals(prop, vertex.getProperty(key));
    }

    @Test
    public void addEdgeProperty() throws Exception {
        PropertyGraph graphOrig = PropertyGraph.fromCollection(graphCollection, graphId);
        ElementId edgeId = graphOrig.getEdges().first().getId();
        String key = "newkey";
        PropertyValue prop = PropertyValue.create("value");
        List<Tuple2<ElementId,PropertyValue>> edgeToPropsList = Collections.singletonList(
                new Tuple2<>(edgeId, prop)
        );
        Dataset<Tuple2<ElementId,PropertyValue>> edgeToProps = spark.createDataset(edgeToPropsList, Encoders.tuple(
                Encoders.bean(ElementId.class),
                Encoders.bean(PropertyValue.class)
        ));
        PropertyGraph graphNew = graphOrig.addEdgeProperty(key, edgeToProps);
        Edge edge = graphNew.getEdges().filter((FilterFunction<Edge>) e ->
                e.getId().equals(edgeId)).first();
        assertEquals(prop, edge.getProperty(key));
    }

    @Test
    public void getEdgeList() throws Exception {
        PropertyGraph graph = PropertyGraph.fromCollection(graphCollection, graphId);
        Dataset<Edge> edges = graph.getEdges();
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

    @Test
    public void getAdjacencyList() throws Exception {
        double[][] expected = {
                {0, 1, 1},
                {0, 0, 1},
                {0, 0, 0}
        };

        GraphCollectionBuilder gcb = new GraphCollectionBuilder();
        ElementId graphId = gcb.addGraphHead(new HashMap<>(), "graph");
        List<ElementId> graphs = Collections.singletonList(graphId);
        ElementId vertexId1 = gcb.addVertex(new HashMap<>(), "vertex1", graphs);
        ElementId vertexId2 = gcb.addVertex(new HashMap<>(), "vertex2", graphs);
        ElementId vertexId3 = gcb.addVertex(new HashMap<>(), "vertex3", graphs);
        List<Tuple2<ElementId,Long>> vertexToIndexList = Arrays.asList(
                new Tuple2<>(vertexId1,0L),
                new Tuple2<>(vertexId2, 1L),
                new Tuple2<>(vertexId3, 2L)
        );
        Dataset<Tuple2<ElementId,Long>> vertexToIndex = spark.createDataset(vertexToIndexList, Encoders.tuple(
                Encoders.bean(ElementId.class),
                Encoders.LONG()
        ));
        gcb.addEdge(vertexId1, vertexId2, new HashMap<>(), "edge12", graphs);
        gcb.addEdge(vertexId1, vertexId3, new HashMap<>(), "edge13", graphs);
        gcb.addEdge(vertexId2, vertexId3, new HashMap<>(), "edge23", graphs);
        PropertyGraph propertyGraph = PropertyGraph.fromCollection(gcb.toGraphCollection(), graphId);
        CoordinateMatrix adjacencyMatrix = propertyGraph.getAdjacencyMatrix(vertexToIndex);
        List<IndexedRow> indexedRows = adjacencyMatrix.toIndexedRowMatrix().rows().toJavaRDD().collect();
        for (IndexedRow row : indexedRows) {
            long index = row.index();
            double[] v = row.vector().toArray();
            for (int j = 0; j < v.length; j++) {
                assertEquals(expected[(int)index][j], v[j], 1e-8);
            }
        }
    }

}