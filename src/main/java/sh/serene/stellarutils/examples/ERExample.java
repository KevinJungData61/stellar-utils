package sh.serene.stellarutils.examples;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.graph.api.StellarBackEndFactory;
import sh.serene.stellarutils.graph.impl.local.LocalBackEndFactory;
import sh.serene.stellarutils.graph.impl.spark.SparkBackEndFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ERExample {
    /**
     * example method - print a list of vertices
     * @param vertices
     */
    public static void printVertices(List<Vertex> vertices) {
        // print all vertices as (id: label, properties)
        for (Vertex v : vertices) {
            ElementId id = v.getId();
            String label = v.getLabel();
            Map<String,PropertyValue> properties = v.getProperties();
            System.out.println(String.format("%s: %s, %s", id.toString(), label, properties.toString()));
        }
    }

    /**
     * example method - index edges by their source node id
     * @param edges
     * @return
     */
    public static Multimap<ElementId,Edge> indexEdgesBySource(List<Edge> edges) {
        Multimap<ElementId,Edge> indexedEdges = ArrayListMultimap.create();
        for (Edge e : edges) {
            ElementId src = e.getSrc();
            indexedEdges.put(src, e);
        }
        return indexedEdges;
    }

    private static List<Edge> runEntityResolution(List<Vertex> vertices, List<Edge> edges) {
        /*
         * ER
         */

        // add a few new edges (v0 -> v1), (v0 -> v2), (v0 -> v3)
        List<Edge> edgesNew = new ArrayList<>();
        edgesNew.add(Edge.create(vertices.get(0).getId(), vertices.get(1).getId(), "new edge"));
        edgesNew.add(Edge.create(vertices.get(0).getId(), vertices.get(2).getId(), "new edge"));
        edgesNew.add(Edge.create(vertices.get(0).getId(), vertices.get(3).getId(), "new edge"));
        return edgesNew;
    }


    private static void execute(
            StellarBackEndFactory beFactory,
            String fileFormat,
            String fileInput,
            String fileOutput
    ) throws IOException {
        /*
         * read graph collection
         */
        StellarGraphCollection graphCollection = beFactory.reader().format(fileFormat).getGraphCollection(fileInput);

        /*
         * get first graph
         */
        StellarGraph graph = graphCollection.get(0);

        /*
         * run ER to get new edges
         */
        List<Edge> edgesNew = runEntityResolution(graph.getVertices().asList(), graph.getEdges().asList());

        /*
         * add new edges to original graph
         */
        StellarGraph graphNew = graph.unionEdges(beFactory.createMemory(edgesNew, Edge.class));

        /*
         * write final graph collection
         */
        graphCollection.union(graphNew).write().json(fileOutput);
    }

    public static void main( String[] args ) {

        /*
         * config
         */
        // with spark backend
        SparkSession sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate();
        StellarBackEndFactory beFactory = new SparkBackEndFactory(sparkSession); //, new SparkHdfsReader(session));
        String fileFormat = "json";
        String fileInput = "small-yelp-hin.epgm";
        String fileOutput = "small-yelp-hin-ER-spark.epgm";
        long start = System.nanoTime();
        try {
            execute(beFactory, fileFormat, fileInput, fileOutput);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.printf("took %,d ns%n", System.nanoTime() - start);

        // with local backend
        StellarBackEndFactory localBeFactory = new LocalBackEndFactory();
        String fileOutputLocal = "small-yelp-hin-ER-local.epgm";
        start = System.nanoTime();
        try {
            execute(localBeFactory, fileFormat, fileInput, fileOutputLocal);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.printf("took %,d ns%n", System.nanoTime() - start);


    }

}
