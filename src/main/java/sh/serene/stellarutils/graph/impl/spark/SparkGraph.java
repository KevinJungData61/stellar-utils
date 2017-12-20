package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.api.StellarEdgeMemory;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;
import sh.serene.stellarutils.graph.api.StellarVertexMemory;

import java.io.Serializable;
import java.util.*;

/**
 * Property Graph - a single graph extracted from an EPGM Graph Collection
 *
 */
public class SparkGraph implements StellarGraph, Serializable {

    /**
     * EPGM Graph Head
     */
    private Dataset<GraphHead> graphHeads;

    /**
     * EPGM Vertices
     */
    private Dataset<Vertex> vertices;

    /**
     * EPGM Edges
     */
    private Dataset<Edge> edges;

    @Deprecated
    public SparkGraph() { }

    private SparkGraph(
            Dataset<GraphHead> graphHeads,
            Dataset<Vertex> vertices,
            Dataset<Edge> edges
    ) {
        this.graphHeads = graphHeads;
        this.vertices = vertices;
        this.edges = edges;
    }

    /**
     * Get Property Graph from collection by ID
     *
     * @param sparkGraphCollection   graph collection
     * @param graphId           ID of property graph
     * @return                  property graph
     */
    public static SparkGraph fromCollection(SparkGraphCollection sparkGraphCollection, ElementId graphId) {

        // check null
        if (sparkGraphCollection == null || graphId == null) {
            return null;
        }

        // check graph head count
        Dataset<GraphHead> graphHeads = sparkGraphCollection.getGraphHeads()
                .filter((FilterFunction<GraphHead>) graphHead1 -> graphHead1.getId().equals(graphId));
        if (graphHeads.count() != 1) {
            return null;
        }

        // get vertices and edges from collection
        Dataset<Vertex> vertices = sparkGraphCollection.getVertices()
                .filter((FilterFunction<VertexCollection>) vertex -> vertex.getGraphs().contains(graphId))
                .map((MapFunction<VertexCollection,Vertex>) Vertex::createFromCollection,
                        Encoders.bean(Vertex.class));
        Dataset<Edge> edges = sparkGraphCollection.getEdges()
                .filter((FilterFunction<EdgeCollection>) edge -> edge.getGraphs().contains(graphId))
                .map((MapFunction<EdgeCollection,Edge>) Edge::createFromCollection,
                        Encoders.bean(Edge.class));

        return new SparkGraph(graphHeads, vertices, edges);
    }

    /**
     * Transform vertices to the EPGM Graph Collection format. This is done by appending the graphID list
     * of each vertex with the current Property Graph's ID.
     *
     * @return  vertices
     */
    private Dataset<VertexCollection> verticesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.vertices
                .map((MapFunction<Vertex,VertexCollection>) vertex -> {
                    List<ElementId> graphs = new ArrayList<>();
                    graphs.add(vertex.version());
                    if (vertex.version() != graphId) {
                        graphs.add(graphId);
                    }
                    return VertexCollection.create(
                            vertex.getId(),
                            vertex.getProperties(),
                            vertex.getLabel(),
                            graphs
                    );
                }, Encoders.bean(VertexCollection.class));
    }

    /**
     * Transform edges to the EPGM Graph Collection format. This is done by appending the graphID list of
     * each edge with the current Property Graph's ID.
     *
     * @return  edges
     */
    private Dataset<EdgeCollection> edgesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.edges
                .map((MapFunction<Edge,EdgeCollection>) edge -> {
                    List<ElementId> graphs = new ArrayList<>();
                    graphs.add(edge.version());
                    if (edge.version() != graphId) {
                        graphs.add(graphId);
                    }
                    return EdgeCollection.create(
                            edge.getId(),
                            edge.getSrc(),
                            edge.getDst(),
                            edge.getProperties(),
                            edge.getLabel(),
                            graphs
                    );
                }, Encoders.bean(EdgeCollection.class));
    }

    /**
     * Turns the Property Graph into a Graph Collection
     *
     * @return  graph collection
     */
    public SparkGraphCollection toCollection() {
        return new SparkGraphCollection(this.graphHeads, verticesToCollection(), edgesToCollection());
    }

    /**
     * Writes the Property Graph into a given Graph Collection
     *
     * @param sparkGraphCollection   graph collection
     * @return                  new graph collection
     */
    public SparkGraphCollection intoCollection(SparkGraphCollection sparkGraphCollection) {
        if (sparkGraphCollection == null) {
            return null;
        }

        Dataset<GraphHead> graphHeads = sparkGraphCollection.getGraphHeads().union(this.graphHeads);
        Dataset<VertexCollection> vertices = sparkGraphCollection.joinVertexCollections(verticesToCollection());
        Dataset<EdgeCollection> edges = sparkGraphCollection.joinEdgeCollections(edgesToCollection());

        return new SparkGraphCollection(graphHeads, vertices, edges);
    }

    /**
     * Get Graph ID
     *
     * @return  graph ID
     */
    public ElementId getGraphId() {
        return this.graphHeads.first().getId();
    }

    /**
     * Get graph heads dataset
     *
     * @return  graph heads
     */
    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    /**
     * Get graph head
     *
     * @return  graph head
     */
    @Override
    public GraphHead getGraphHead() {
        return this.graphHeads.first();
    }

    /**
     * Get vertices
     *
     * @return  vertices
     */
    @Override
    public StellarGraphMemory<Vertex> getVertices() {
        return new SparkGraphMemory<>(this.vertices);
    }

    /**
     * Get edges
     *
     * @return  edges
     */
    @Override
    public StellarGraphMemory<Edge> getEdges() {
        return new SparkGraphMemory<>(this.edges);
    }

    /**
     * Union two stellar graphs
     *
     * @param other     other graph
     * @return          union of graphs
     */
    @Override
    public StellarGraph union(StellarGraph other) {
        //TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Union a set of vertices into current graph
     *
     * @param vertices vertices
     * @return new graph
     */
    @Override
    public StellarGraph unionVertices(StellarGraphMemory<Vertex> vertices) {
        return new SparkGraph(createGraphHead(), this.vertices.union(vertices.asDataset()), this.edges);
    }

    /**
     * Union a set of edges into current graph
     *
     * @param edges edges
     * @return new graph
     */
    @Override
    public StellarGraph unionEdges(StellarGraphMemory<Edge> edges) {
        return new SparkGraph(createGraphHead(), this.vertices, this.edges.union(edges.asDataset()));
    }

    @Override
    public StellarGraph union(StellarEdgeMemory edgeMemory) {
        return new SparkGraph(createGraphHead(), this.vertices, this.edges.union(edgeMemory.asDataset()));
    }

    @Override
    public StellarGraph union(StellarVertexMemory vertexMemory) {
        return new SparkGraph(createGraphHead(), this.vertices.union(vertexMemory.asDataset()), this.edges);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof SparkGraph) && ((SparkGraph) obj).getGraphId().equals(this.getGraphId());
    }

    /**
     * Create a new graph head with the same properties as the current graph head. A new ID is generated.
     *
     * @return  graph head
     */
    private Dataset<GraphHead> createGraphHead() {
        GraphHead graphHeadNew = this.getGraphHead().copy();
        return this.getGraphHeads().map(
                (MapFunction<GraphHead,GraphHead>) g -> graphHeadNew,
                Encoders.bean(GraphHead.class)
        );
    }

    /**
     * Add edges to graph. No check is performed to ensure the edges are valid for the current graph, so
     * it is up to the user to make sure this is the case
     *
     * @param edges     new edges to add
     * @return          new graph
     */
    public SparkGraph addEdges(Dataset<Edge> edges) {
        return new SparkGraph(createGraphHead(), this.vertices, this.edges.union(edges));
    }

    /**
     * Add vertices to graph. No check is performed to ensure the vertices are valid for the current graph,
     * so it is up to the user to make sure this is the case
     *
     * @param vertices      new vertices to add
     * @return              new graph
     */
    public SparkGraph addVertices(Dataset<Vertex> vertices) {
        return new SparkGraph(createGraphHead(), this.vertices.union(vertices), this.edges);
    }

    /**
     * Add properties to vertices
     *
     * @param key               property key
     * @param vertexToProps     dataset of (vertex ID, property value)
     * @return                  new property graph
     */
    public SparkGraph addVertexProperty(String key, Dataset<Tuple2<ElementId,PropertyValue>> vertexToProps) {
        Dataset<GraphHead> graphHeadNew = createGraphHead();
        ElementId graphId = graphHeadNew.first().getId();
        Dataset<Vertex> verticesNew = this.vertices
                .joinWith(
                        vertexToProps,
                        this.vertices.col("id").equalTo(vertexToProps.col("_1")),
                        "leftouter"
                )
                .map((MapFunction<
                        Tuple2<Vertex,
                            Tuple2<ElementId,PropertyValue>>,
                        Vertex>) tup -> {
                    Vertex vertexOri = tup._1;
                    if (tup._2 == null) {
                        return vertexOri;
                    }
                    Map<String,PropertyValue> properties = new HashMap<>(vertexOri.getProperties());
                    properties.put(key, tup._2._2);
                    return Vertex.create(
                            vertexOri.getId(),
                            properties,
                            vertexOri.getLabel(),
                            graphId
                    );
                }, Encoders.bean(Vertex.class));
        return new SparkGraph(graphHeadNew, verticesNew, this.edges);
    }

    /**
     * Add properties to edges
     *
     * @param key               property key
     * @param edgeToProps       dataset of (edge ID, property value)
     * @return                  new property graph
     */
    public SparkGraph addEdgeProperty(String key, Dataset<Tuple2<ElementId,PropertyValue>> edgeToProps) {
        Dataset<GraphHead> graphHeadNew = createGraphHead();
        ElementId graphId = graphHeadNew.first().getId();
        Dataset<Edge> edgesNew = this.edges
                .joinWith(
                        edgeToProps,
                        this.edges.col("id").equalTo(edgeToProps.col("_1")),
                        "leftouter"
                )
                .map((MapFunction<
                        Tuple2<Edge,
                            Tuple2<ElementId,PropertyValue>>,
                        Edge>) tup -> {
                    Edge edgeOri = tup._1;
                    if (tup._2 == null) {
                        return edgeOri;
                    }
                    Map<String,PropertyValue> properties = new HashMap<>(edgeOri.getProperties());
                    properties.put(key, tup._2._2);
                    return Edge.create(
                            edgeOri.getId(),
                            edgeOri.getSrc(),
                            edgeOri.getDst(),
                            properties,
                            edgeOri.getLabel(),
                            graphId
                    );
                }, Encoders.bean(Edge.class));
        return new SparkGraph(graphHeadNew, this.vertices, edgesNew);
    }

    /**
     * Get edge list as a dataset of tuples (src,dst) from graph.
     *
     * @return  edge list
     */
    public StellarGraphMemory<Tuple2<ElementId,ElementId>> getEdgeList() {
        return new SparkGraphMemory<>(this.edges.map((MapFunction<Edge,Tuple2<ElementId,ElementId>>) edge -> (
                new Tuple2<>(edge.getSrc(), edge.getDst())
                ), Encoders.tuple(Encoders.bean(ElementId.class), Encoders.bean(ElementId.class))));
    }

    /**
     * Get edge list as a dataset of tuples (src,dst) where src and dst are vertex indices defined by a given dataset
     * vertexToIndex.
     *
     * @param vertexToIndex     mapping from vertex id to vertex index
     * @return                  edge list as index pairs
     */
    public Dataset<Tuple2<Long,Long>> getIndexPairList(Dataset<Tuple2<ElementId,Long>> vertexToIndex) {
        Dataset<Tuple2<ElementId,ElementId>> edgeList = getEdgeList().asDataset();
        Dataset<Tuple2<Long,ElementId>> srcesIndexed = edgeList
                .joinWith(
                        vertexToIndex,
                        edgeList.col("_1").equalTo(vertexToIndex.col("_1")),
                        "inner"
                )
                .map((MapFunction<
                        Tuple2<Tuple2<ElementId,ElementId>,Tuple2<ElementId,Long>>,
                        Tuple2<Long,ElementId>>) tup -> (new Tuple2<>(tup._2._2, tup._1._2))
                , Encoders.tuple(Encoders.LONG(), Encoders.bean(ElementId.class)));
        return srcesIndexed
                .joinWith(
                        vertexToIndex,
                        srcesIndexed.col("_2").equalTo(vertexToIndex.col("_1")),
                        "inner"
                )
                .map((MapFunction<
                        Tuple2<Tuple2<Long,ElementId>,Tuple2<ElementId,Long>>,
                        Tuple2<Long,Long>>) tup -> (new Tuple2<>(tup._1._1, tup._2._2)),
                        Encoders.tuple(Encoders.LONG(), Encoders.LONG()));
    }

    /**
     * Get adjacency matrix
     *
     * @param vertexToIndex     mapping from vertex id to vertex index
     * @return                  adjacency matrix
     */
    public CoordinateMatrix getAdjacencyMatrix(Dataset<Tuple2<ElementId,Long>> vertexToIndex) {
        JavaRDD<MatrixEntry> entries = getIndexPairList(vertexToIndex)
                .toJavaRDD()
                .map(edge -> (new MatrixEntry(edge._1, edge._2, 1)));
        return new CoordinateMatrix(entries.rdd());
    }

    /**
     * Get Laplacian matrix
     *
     * @param vertexToIndex     mapping from vertex id to vertex index
     * @return                  Laplacian matrix
     */
    public CoordinateMatrix getLaplacianMatrix(Dataset<Tuple2<ElementId,Long>> vertexToIndex) {
        JavaRDD<MatrixEntry> entries = getIndexPairList(vertexToIndex)
                .toJavaRDD()
                .mapToPair(tup -> new Tuple2<>(tup._1, Collections.singletonList(tup._2)))
                .reduceByKey((l1, l2) -> {
                    List<Long> l3 = new ArrayList<>(l1);
                    l3.addAll(l2);
                    return l3;
                })
                .map(edges -> (new MatrixEntry(edges._1, edges._1, edges._2.size())))
                .union(
                        getIndexPairList(vertexToIndex)
                                .toJavaRDD()
                                .map(edge -> (new MatrixEntry(edge._1, edge._2, -1)))
                );
        return new CoordinateMatrix(entries.rdd());
    }

}
