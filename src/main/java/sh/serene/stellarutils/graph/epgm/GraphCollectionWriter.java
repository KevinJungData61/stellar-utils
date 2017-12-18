package sh.serene.stellarutils.graph.epgm;

import sh.serene.stellarutils.io.gdf.GDFDataSink;
import sh.serene.stellarutils.io.json.JSONDataSink;
import sh.serene.stellarutils.io.parquet.ParquetDataSink;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;

/**
 * Writes a Graph Collection in various supported formats.
 *
 */
public class GraphCollectionWriter {

    /**
     * Graph collection to write
     */
    private final SparkGraphCollection sparkGraphCollection;

    /**
     * Constructor
     *
     * @param sparkGraphCollection
     */
    public GraphCollectionWriter(SparkGraphCollection sparkGraphCollection) {
        this.sparkGraphCollection = sparkGraphCollection;
    }

    /**
     * Writes the graph collection in parquet format
     *
     * @param path  output path
     * @return      success
     */
    public boolean parquet(String path) {
        ParquetDataSink parquetDataSink = new ParquetDataSink(path);
        return parquetDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in JSON format
     *
     * @param path  output path
     * @return      success
     */
    public boolean json(String path) {
        JSONDataSink jsonDataSink = new JSONDataSink(path);
        return jsonDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in GDF format
     *
     * @param path  output path
     * @return      success
     */
    public boolean gdf(String path) {
        GDFDataSink gdfDataSink = new GDFDataSink(path);
        return gdfDataSink.writeGraphCollection(sparkGraphCollection);
    }
}
