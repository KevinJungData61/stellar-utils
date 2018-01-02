package sh.serene.stellarutils.graph.impl.local;

import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.local.LocalWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalGraphCollection implements StellarGraphCollection {

    List<GraphHead> graphHeads;
    List<VertexCollection> vertices;
    List<EdgeCollection> edges;

    public LocalGraphCollection() { }

    public LocalGraphCollection(
            List<GraphHead> graphHeads, List<VertexCollection> vertices, List<EdgeCollection> edges) {
        this.graphHeads = new ArrayList<>(graphHeads);
        this.vertices = new ArrayList<>(vertices);
        this.edges = new ArrayList<>(edges);
    }

    /**
     * Create a writer object
     *
     * @return graph collection writer
     */
    @Override
    public LocalWriter write() {
        return new LocalWriter(this);
    }

    /**
     * Get graph at index
     *
     * @param index index
     * @return graph
     */
    @Override
    public LocalGraph get(int index) {
        GraphHead graphHead = this.graphHeads.get(index);
        if (graphHead == null) {
            return null;
        }
        ElementId graphId = graphHead.getId();

        // get vertices
        List<Vertex> vertices = new ArrayList<>();
        for (VertexCollection vc : this.vertices) {
            if (vc.getGraphs().contains(graphId)) {
                vertices.add(Vertex.createFromCollection(vc));
            }
        }

        // get edges
        List<Edge> edges = new ArrayList<>();
        for (EdgeCollection ec : this.edges) {
            if (ec.getGraphs().contains(graphId)) {
                edges.add(Edge.createFromCollection(ec));
            }
        }

        return new LocalGraph(graphHead, vertices, edges);
    }

    /**
     * Get graph by ID
     *
     * @param graphId graph ID
     * @return graph
     */
    @Override
    public LocalGraph get(ElementId graphId) {
        // check null
        if (graphId == null) {
            return null;
        }

        for (int i = 0; i < this.graphHeads.size(); i++) {
            if (this.graphHeads.get(i).getId().equals(graphId)) {
                return get(i);
            }
        }
        return null;
    }

    /**
     * Union a graph into this graph collection
     *
     * @param graph graph
     * @return new graph collection
     */
    @Override
    public LocalGraphCollection union(StellarGraph graph) {
        // union with nothing is itself
        if (graph == null) {
            return this;
        }

        return union(graph.toCollection());
    }

    /**
     * Union two graph collections
     *
     * @param other other graph collection
     * @return new graph collection
     */
    @Override
    public LocalGraphCollection union(StellarGraphCollection other) {
        if (other == null) {
            return this;
        } else if (other instanceof LocalGraphCollection) {
            LocalGraphCollection otherLocal = (LocalGraphCollection) other;
            List<GraphHead> graphHeads = new ArrayList<>(this.graphHeads);
            graphHeads.addAll(otherLocal.graphHeads);
            List<VertexCollection> vertices = joinVertexCollections(otherLocal.vertices);
            List<EdgeCollection> edges = joinEdgeCollections(otherLocal.edges);
            return new LocalGraphCollection(graphHeads, vertices, edges);
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }



    /**
     * Concatenate two element IDs into a byte array
     *
     * @param id1   element ID 1
     * @param id2   element ID 2
     * @return      byte array
     */
    private byte[] concatenateIds(ElementId id1, ElementId id2) {
        byte[] bytes = new byte[id1.getBytes().length + id2.getBytes().length];
        System.arraycopy(id1.getBytes(), 0, bytes, 0, id1.getBytes().length);
        System.arraycopy(id2.getBytes(), 0, bytes, id1.getBytes().length, id2.getBytes().length);
        return bytes;
    }

    private <T extends Element> Map<byte[],T> buildElemMap(List<T> elems) {
        Map<byte[],T> map = new HashMap<>(elems.size());
        for (T e : elems) {
            map.put(concatenateIds(e.getId(), e.version()), e);
        }
        return map;
    }

    /**
     * Full-outer join vertices with another set of vertices based on ID and version indicated by the graph IDs
     *
     * @param vOther    other vertices
     * @return          new vertices
     */
    public List<VertexCollection> joinVertexCollections(List<VertexCollection> vOther) {

        Map<byte[],VertexCollection> v1 = buildElemMap(this.vertices);
        Map<byte[],VertexCollection> v2 = buildElemMap(vOther);
        List<VertexCollection> verticesJoined = new ArrayList<>();

        for (Map.Entry<byte[],VertexCollection> entry : v1.entrySet()) {
            byte[] key = entry.getKey();
            VertexCollection v = entry.getValue();
            if (v2.containsKey(key)) {
                List<ElementId> graphs = new ArrayList<>(v.getGraphs());
                for (ElementId graphId : v2.get(key).getGraphs()) {
                    if (!graphs.contains(graphId)) {
                        graphs.add(graphId);
                    }
                }
                v2.remove(key);
                verticesJoined.add(VertexCollection.create(
                        v.getId(),
                        v.getProperties(),
                        v.getLabel(),
                        graphs
                ));
            } else {
                verticesJoined.add(v);
            }
        }
        for (Map.Entry<byte[],VertexCollection> entry : v2.entrySet()) {
            verticesJoined.add(entry.getValue());
        }

        return verticesJoined;
    }

    /**
     * Full-outer join edges with another set of edges based on ID and version indicated by the graph IDs
     *
     * @param eOther    other edges
     * @return          new edges
     */
    public List<EdgeCollection> joinEdgeCollections(List<EdgeCollection> eOther) {

        Map<byte[],EdgeCollection> e1 = buildElemMap(this.edges);
        Map<byte[],EdgeCollection> e2 = buildElemMap(eOther);
        List<EdgeCollection> edgesJoined = new ArrayList<>();

        for (Map.Entry<byte[],EdgeCollection> entry : e1.entrySet()) {
            byte[] key = entry.getKey();
            EdgeCollection e = entry.getValue();
            if (e2.containsKey(key)) {
                List<ElementId> graphs = new ArrayList<>(e.getGraphs());
                for (ElementId graphId : e2.get(key).getGraphs()) {
                    if (!graphs.contains(graphId)) {
                        graphs.add(graphId);
                    }
                }
                e2.remove(key);
                edgesJoined.add(EdgeCollection.create(
                        e.getId(),
                        e.getSrc(),
                        e.getDst(),
                        e.getProperties(),
                        e.getLabel(),
                        graphs
                ));
            } else {
                edgesJoined.add(e);
            }
        }
        for (Map.Entry<byte[],EdgeCollection> entry : e2.entrySet()) {
            edgesJoined.add(entry.getValue());
        }

        return edgesJoined;
    }
}
