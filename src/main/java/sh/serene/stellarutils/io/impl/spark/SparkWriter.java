package sh.serene.stellarutils.io.impl.spark;

import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.spark.gdf.GDFDataSink;
import sh.serene.stellarutils.io.impl.spark.json.JSONDataSink;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSink;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

import java.io.IOException;

/**
 * Writes a Graph Collection in various supported formats.
 *
 */
public class SparkWriter implements StellarWriter {

    /**
     * Graph collection to write
     */
    private final SparkGraphCollection sparkGraphCollection;
    private final String path;

    /**
     * Constructor
     *
     * @param sparkGraphCollection
     */
    public SparkWriter(SparkGraphCollection sparkGraphCollection, String path) {
        this.sparkGraphCollection = sparkGraphCollection;
        this.path = path;
    }

    /**
     * Writes the graph collection in parquet format
     *
     * @return      success
     */
    public boolean parquet() {
        ParquetDataSink parquetDataSink = new ParquetDataSink(path);
        return parquetDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in JSON format
     *
     * @return      success
     */
    public boolean json() {
        JSONDataSink jsonDataSink = new JSONDataSink(path);
        return jsonDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in GDF format
     *
     * @return      success
     */
    public boolean gdf() {
        GDFDataSink gdfDataSink = new GDFDataSink(path);
        return gdfDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Write graph collection to given path.
     *
     * @param fileFormat file format
     * @return success
     */
    @Override
    public boolean format(String fileFormat) throws IOException {
        switch (fileFormat) {
            case "json":
                return this.json();
            case "parquet":
                return this.parquet();
            case "gdf":
                return this.gdf();
            default:
                throw new IOException("Invalid file format: " + fileFormat);
        }
    }
}
