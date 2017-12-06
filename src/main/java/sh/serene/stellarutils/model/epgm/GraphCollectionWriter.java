package sh.serene.stellarutils.model.epgm;

import sh.serene.stellarutils.io.gdf.GDFDataSink;
import sh.serene.stellarutils.io.json.JSONDataSink;
import sh.serene.stellarutils.io.parquet.ParquetDataSink;

/**
 * Writes a Graph Collection in various supported formats.
 *
 */
public class GraphCollectionWriter {

    /**
     * Graph collection to write
     */
    private final GraphCollection graphCollection;

    /**
     * Constructor
     *
     * @param graphCollection
     */
    GraphCollectionWriter(GraphCollection graphCollection) {
        this.graphCollection = graphCollection;
    }

    /**
     * Writes the graph collection in parquet format
     *
     * @param path  output path
     * @return      success
     */
    public boolean parquet(String path) {
        ParquetDataSink parquetDataSink = new ParquetDataSink(path);
        return parquetDataSink.writeGraphCollection(graphCollection);
    }

    /**
     * Writes the graph collection in JSON format
     *
     * @param path  output path
     * @return      success
     */
    public boolean json(String path) {
        JSONDataSink jsonDataSink = new JSONDataSink(path);
        return jsonDataSink.writeGraphCollection(graphCollection);
    }

    /**
     * Writes the graph collection in GDF format
     *
     * @param path  output path
     * @return      success
     */
    public boolean gdf(String path) {
        GDFDataSink gdfDataSink = new GDFDataSink(path);
        return gdfDataSink.writeGraphCollection(graphCollection);
    }
}
