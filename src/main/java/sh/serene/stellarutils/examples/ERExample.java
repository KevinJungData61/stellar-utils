package sh.serene.stellarutils.examples;

import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.graph.api.StellarBackEndFactory;
import sh.serene.stellarutils.graph.impl.local.LocalBackEndFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ERExample {

    /**
     * helper class to contain ER Row info
     */
    private static class ERKey {
        public final String name;
        public final List<String> wrote;
        public final String worksFor;

        public ERKey(String name, List<String> wrote, String worksFor) {
            this.name = name;
            this.wrote = new ArrayList<>(wrote);
            this.worksFor = worksFor;
        }

        public int hashCode() {
            return Objects.hash(name, wrote, worksFor);
        }

        public boolean equals(Object other) {
            if (other instanceof ERKey) {
                ERKey otherk = (ERKey) other;
                return (Objects.equals(this.name, otherk.name) &&
                        Objects.equals(this.wrote, otherk.wrote) &&
                        Objects.equals(this.worksFor, otherk.worksFor)
                );
            } else {
                return false;
            }
        }
    }

    /**
     * Creates new edges for duplicate entities
     *
     * @param tuples        adjacency tuples
     * @param tuple2Key     map function AdjacencyTuple -> ERKey
     * @return              list of new edges
     */
    private static List<Edge> runEntityResolution(List<AdjacencyTuple> tuples, Function<AdjacencyTuple,ERKey> tuple2Key) {

        List<Edge> edgesNew = new ArrayList<>();

        // group by ER key to group duplicates
        Map<ERKey,List<AdjacencyTuple>> resolved = tuples.stream().collect(Collectors.groupingBy(tuple2Key));

        // for all pairs of duplicates, add edges between them
        resolved.forEach((key, dups) -> {
            if (dups.size() > 1) {
                for (int i = 0; i < dups.size(); i++) {
                    for (int j = i+1; j < dups.size(); j++) {
                        ElementId id1 = dups.get(i).source.getId();
                        ElementId id2 = dups.get(j).source.getId();
                        edgesNew.add(Edge.create(id1, id2, "duplicateOf"));
                        edgesNew.add(Edge.create(id2, id1, "duplicateOf"));
                    }
                }
            }
        });

        return edgesNew;
    }

    /**
     * Reads from input file, performs ER, writes to output file
     *
     * @param beFactory         backend factory
     * @param fileFormat        input/output file format
     * @param fileInput         input file path
     * @param fileOutput        output file path
     * @param vertexPredicate   vertex predicate to create ER rows with
     * @param tuple2Key         map function AdjacencyTuple -> ERKey
     * @throws IOException
     */
    private static void execute(
            StellarBackEndFactory beFactory,
            String fileFormat,
            String fileInput,
            String fileOutput,
            Predicate<Vertex> vertexPredicate,
            Function<AdjacencyTuple,ERKey> tuple2Key
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
         * get rows to perform ER with, where each row has the information of a user node + its neighbours
         */
        List<AdjacencyTuple> tuples = graph.getAdjacencyTuples(vertexPredicate);

        /*
         * run ER to get new edges
         */
        List<Edge> edgesNew = runEntityResolution(tuples, tuple2Key);

        /*
         * add new edges to original graph
         */
        StellarGraph graphNew = graph.unionEdges(beFactory.createMemory(edgesNew, Edge.class));

        /*
         * write final graph collection
         */
        graphCollection.union(graphNew).write().json(fileOutput);
    }

    /**
     * run me
     *
     * @param args
     */
    public static void main( String[] args ) {

        /*
         create local backend
          */
        StellarBackEndFactory localBeFactory = new LocalBackEndFactory(); // new SparkBackEndFactory(sparkSession);

        /*
         file format / paths
          */
        String fileFormat = "json";
        String fileInput = "papers.epgm";
        String fileOutputLocal = "papers-ER.epgm";

        /*
         ER config

         mocked for a graph containing data that resembles:
              (ResearchGroup {name}) <--[worksFor]-- (Author {name}) <--[writtenBy]-- (Paper {title})
         to be turned into rows for each author containing:
              { name: String, wrote: [String], worksFor: String }
          */

        // type of vertices to be used as "rows" that are being resolved with ER
        Predicate<Vertex> vertexPredicate = v -> v.getLabel().equals("Author");

        // map function to transform tuple to ER key to group by
        Function<AdjacencyTuple,ERKey> tuple2Key = tuple -> {
            List<String> worksFor = tuple.outbound.entrySet().stream()
                    .filter(entry -> entry.getKey().getLabel().equals("worksFor"))
                    .map(entry -> entry.getValue().getPropertyValue("name", String.class))
                    .collect(Collectors.toList());
            return new ERKey(
                    tuple.source.getPropertyValue("name", String.class),
                    tuple.inbound.entrySet().stream()
                            .filter(entry -> entry.getKey().getLabel().equals("writtenBy"))
                            .map(entry -> entry.getValue().getPropertyValue("title", String.class))
                            .collect(Collectors.toList()),
                    (worksFor.size() == 1) ? worksFor.get(0) : ""
            );
        };

        try {
            execute(localBeFactory, fileFormat, fileInput, fileOutputLocal, vertexPredicate, tuple2Key);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
