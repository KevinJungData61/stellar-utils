package sh.serene.stellarutils.io.impl.local;

import org.json.simple.parser.JSONParser;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.GraphHead;
import sh.serene.stellarutils.entities.VertexCollection;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.local.json.JSONToLocal;
import sh.serene.stellarutils.io.impl.spark.json.JSONConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Basic Graph Collection Reader
 *
 */
public class LocalReader implements StellarReader {

    private final String fileFormat;

    public LocalReader() {
        this.fileFormat = "json";
    }

    private LocalReader(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    /**
     * Set file format. Supported formats may vary depending on implementation
     *
     * @param fileFormat file format
     * @return reader object
     */
    @Override
    public LocalReader format(String fileFormat) {
        return new LocalReader(fileFormat);
    }

    /**
     * Read graph collection from path.
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public LocalGraphCollection getGraphCollection(String path) throws IOException {
        switch (this.fileFormat.toLowerCase()) {
            case "json":
                return json(path);
            case "parquet":
                return parquet(path);
            default:
                throw new UnsupportedOperationException("Invalid file format: " + fileFormat);
        }
    }

    /**
     * Read graph collection from path in json format. This takes precedence over any previous file format setting
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public LocalGraphCollection json(String path) {
        path = Objects.requireNonNull(path);
        if (path.charAt(path.length() - 1) != '/') {
            path += '/';
        }
        try {
            List<VertexCollection> vertexCollectionList = Files
                    .lines(Paths.get(path + JSONConstants.VERTICES_FILE))
                    .map(JSONToLocal.toVertexCollection())
                    .collect(Collectors.toList());
            List<EdgeCollection> edgeCollectionList = Files
                    .lines(Paths.get(path + JSONConstants.EDGES_FILE))
                    .map(JSONToLocal.toEdgeCollection())
                    .collect(Collectors.toList());
            List<GraphHead> graphHeadList = Files
                    .lines(Paths.get(path + JSONConstants.GRAPHS_FILE))
                    .map(JSONToLocal.toGraphHead())
                    .collect(Collectors.toList());
            return new LocalGraphCollection(graphHeadList, vertexCollectionList, edgeCollectionList);
        } catch (IOException e) {
            //TODO
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Read graph colleciton from path in parquet format. This takes precedence over any previous file format setting
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public LocalGraphCollection parquet(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
