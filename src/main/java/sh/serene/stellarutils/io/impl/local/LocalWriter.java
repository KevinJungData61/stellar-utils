package sh.serene.stellarutils.io.impl.local;

import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.local.json.LocalToJSON;
import sh.serene.stellarutils.io.impl.spark.json.JSONConstants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LocalWriter implements StellarWriter {

    private final LocalGraphCollection graphCollection;
    private final String fileFormat;

    public LocalWriter(LocalGraphCollection graphCollection) {
        if (graphCollection == null) {
            throw new NullPointerException("graph collection was null");
        }
        this.graphCollection = graphCollection;
        this.fileFormat = "json";
    }

    private LocalWriter(LocalGraphCollection graphCollection, String fileFormat) {
        this.graphCollection = graphCollection;
        this.fileFormat = fileFormat;
    }

    /**
     * Set file format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat file format
     * @return writer object
     */
    @Override
    public StellarWriter format(String fileFormat) {
        return new LocalWriter(this.graphCollection, fileFormat);
    }

    /**
     * Save graph collection to path.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean save(String path) throws IOException {
        switch (this.fileFormat.toLowerCase()) {
            case "json":
                return json(path);
            case "parquet":
                return parquet(path);
            case "gdf":
                return gdf(path);
            default:
                throw new UnsupportedOperationException("unsupported file format: " + this.fileFormat);
        }
    }

    /**
     * Save graph collection to path in json format. This takes precedence over any previous file format setting.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean json(String path) {
        path = Objects.requireNonNull(path, "Given path was null");
        if (path.charAt(path.length() - 1) != '/') {
            path += '/';
        }
        String jsonGraphHeads = graphCollection.getGraphHeads().stream()
                .map(LocalToJSON.fromGraphHead())
                .collect(Collectors.joining("\n")).trim();
        String jsonVertexCollections = graphCollection.getVertices().stream()
                .map(LocalToJSON.fromVertexCollection())
                .collect(Collectors.joining("\n")).trim();
        String jsonEdgeCollections = graphCollection.getEdges().stream()
                .map(LocalToJSON.fromEdgeCollection())
                .collect(Collectors.joining("\n")).trim();

        try {
            new File(path).mkdirs();
            FileWriter graphHeadFile = new FileWriter(path + JSONConstants.GRAPHS_FILE);
            graphHeadFile.write(jsonGraphHeads);
            graphHeadFile.flush();
            FileWriter vertexFile = new FileWriter(path + JSONConstants.VERTICES_FILE);
            vertexFile.write(jsonVertexCollections);
            vertexFile.flush();
            FileWriter edgeFile = new FileWriter(path + JSONConstants.EDGES_FILE);
            edgeFile.write(jsonEdgeCollections);
            edgeFile.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Save graph collection to path in parquet format. This takes precedence over any previous file format setting.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean parquet(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Save graph collection to path in gdf format. This takes precedence over any previous file format setting.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean gdf(String path) {

        List<String> vertexProps = graphCollection.getVertices().stream().flatMap(
                v -> v.getProperties().keySet().stream()
            ).distinct().collect(Collectors.toList());
        List<String> edgeProps = graphCollection.getEdges().stream().flatMap(
                e -> e.getProperties().keySet().stream()
            ).distinct().collect(Collectors.toList());

        try {
            FileWriter writer = new FileWriter(path);

            // vertices header
            writer.write(String.format(
                    "nodedef>name VARCHAR,label VARCHAR,type VARCHAR%s%n",
                    vertexProps.stream().map(s -> "," + s + " VARCHAR").collect(Collectors.joining(""))
            ));

            // vertices
            writer.write(
                    graphCollection.getVertices().stream()
                        .map(v -> String.format(
                                "%s,%s,%s%s%n",
                                v.getId().toString(), v.getLabel(), v.getLabel(),
                                vertexProps.stream().map(
                                        key -> "," + v.getProperties().getOrDefault(key, PropertyValue.create("")).toString()
                                ).collect(Collectors.joining(""))
                        )).collect(Collectors.joining(""))
            );

            // edges header
            writer.write(String.format(
                    "edgedef>node1 VARCHAR,node2 VARCHAR,directed BOOLEAN,label VARCHAR,type VARCHAR%s%n",
                    edgeProps.stream().map(s -> "," + s + " VARCHAR").collect(Collectors.joining(""))
            ));

            // edges
            writer.write(
                    graphCollection.getEdges().stream()
                            .map(e -> String.format(
                                    "%s,%s,true,%s,%s%s%n",
                                    e.getSrc().toString(), e.getDst().toString(), e.getLabel(), e.getLabel(),
                                    edgeProps.stream().map(
                                            key -> "," + e.getProperties().getOrDefault(key, PropertyValue.create("")).toString()
                                    ).collect(Collectors.joining(""))
                            )).collect(Collectors.joining(""))
            );
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
