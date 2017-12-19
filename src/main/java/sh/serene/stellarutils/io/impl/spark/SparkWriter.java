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
    private final String fileFormat;

    /**
     * Constructor
     *
     * @param sparkGraphCollection
     */
    public SparkWriter(SparkGraphCollection sparkGraphCollection) {
        this.sparkGraphCollection = sparkGraphCollection;
        this.fileFormat = "json";
    }

    private SparkWriter(SparkGraphCollection sparkGraphCollection, String fileFormat) {
        this.sparkGraphCollection = sparkGraphCollection;
        this.fileFormat = fileFormat;
    }

    /**
     * Writes the graph collection in parquet format
     *
     * @return      success
     */
    @Override
    public boolean parquet(String path) {
        ParquetDataSink parquetDataSink = new ParquetDataSink(path);
        return parquetDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in JSON format
     *
     * @return      success
     */
    @Override
    public boolean json(String path) {
        JSONDataSink jsonDataSink = new JSONDataSink(path);
        return jsonDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Writes the graph collection in GDF format
     *
     * @return      success
     */
    public boolean gdf(String path) {
        GDFDataSink gdfDataSink = new GDFDataSink(path);
        return gdfDataSink.writeGraphCollection(sparkGraphCollection);
    }

    /**
     * Set file format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat file format
     * @return writer object
     */
    @Override
    public StellarWriter format(String fileFormat) {
        return new SparkWriter(this.sparkGraphCollection, fileFormat);
    }

    /**
     * Write graph collection to given path.
     *
     * @param path  output path
     * @return success
     */
    @Override
    public boolean save(String path) throws IOException {
        switch (fileFormat.toLowerCase()) {
            case "json":
                return this.json(path);
            case "parquet":
                return this.parquet(path);
            case "gdf":
                return this.gdf(path);
            default:
                throw new IOException("Invalid file format: " + fileFormat);
        }
    }
}
