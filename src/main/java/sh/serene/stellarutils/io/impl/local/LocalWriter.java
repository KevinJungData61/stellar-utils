package sh.serene.stellarutils.io.impl.local;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.spark.json.JSONConstants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

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
        if (path.charAt(path.length() - 1) != '/') {
            path += '/';
        }
        StringBuilder sbGraphHeads = new StringBuilder();
        for (GraphHead g : graphCollection.getGraphHeads()) {
            JSONObject json = new JSONObject();
            JSONObject props = new JSONObject();
            for (Map.Entry<String,PropertyValue> entry : g.getProperties().entrySet()) {
                props.put(entry.getKey(), entry.getValue().value());
            }
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, g.getLabel());
            json.put(JSONConstants.IDENTIFIER, g.getId().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            sbGraphHeads.append(json.toString() + "\n");
        }

        StringBuilder sbVertexCollections = new StringBuilder();
        for (VertexCollection v : graphCollection.getVertices()) {
            JSONObject json = new JSONObject();
            JSONObject props = new JSONObject();
            for (Map.Entry<String,PropertyValue> entry : v.getProperties().entrySet()) {
                props.put(entry.getKey(), entry.getValue().value());
            }
            JSONArray graphs = new JSONArray();
            for (ElementId graphId : v.getGraphs()) {
                graphs.add(graphId.toString());
            }
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, v.getLabel());
            meta.put(JSONConstants.GRAPHS, graphs);
            json.put(JSONConstants.IDENTIFIER, v.getId().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            sbVertexCollections.append(json.toString() + "\n");
        }

        StringBuilder sbEdgeCollections = new StringBuilder();
        for (EdgeCollection e : graphCollection.getEdges()) {
            JSONObject json = new JSONObject();
            JSONObject props = new JSONObject();
            for (Map.Entry<String,PropertyValue> entry : e.getProperties().entrySet()) {
                props.put(entry.getKey(), entry.getValue().value());
            }
            JSONArray graphs = new JSONArray();
            for (ElementId graphId : e.getGraphs()) {
                graphs.add(graphId.toString());
            }
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, e.getLabel());
            meta.put(JSONConstants.GRAPHS, graphs);
            json.put(JSONConstants.IDENTIFIER, e.getId().toString());
            json.put(JSONConstants.SOURCE, e.getSrc().toString());
            json.put(JSONConstants.TARGET, e.getDst().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            sbEdgeCollections.append(json.toString() + "\n");
        }

        try {
            new File(path).mkdirs();
            FileWriter graphHeadFile = new FileWriter(path + JSONConstants.GRAPHS_FILE);
            graphHeadFile.write(sbGraphHeads.toString().trim());
            graphHeadFile.flush();
            FileWriter vertexFile = new FileWriter(path + JSONConstants.VERTICES_FILE);
            vertexFile.write(sbVertexCollections.toString().trim());
            vertexFile.flush();
            FileWriter edgeFile = new FileWriter(path + JSONConstants.EDGES_FILE);
            edgeFile.write(sbEdgeCollections.toString().trim());
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
}
