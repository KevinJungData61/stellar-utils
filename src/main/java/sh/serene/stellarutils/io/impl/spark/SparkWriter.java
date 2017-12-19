package sh.serene.stellarutils.io.impl.spark;

import sh.serene.stellarutils.io.impl.spark.gdf.GDFDataSink;
import sh.serene.stellarutils.io.impl.spark.json.JSONDataSink;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSink;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

/**
 * Writes a Graph Collection in various supported formats.
 *
 */
public class SparkWriter {

    /**
     * Graph collection to write
     */
    private final SparkGraphCollection sparkGraphCollection;

    /**
     * Constructor
     *
     * @param sparkGraphCollection
     */
    public SparkWriter(SparkGraphCollection sparkGraphCollection) {
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
