package sh.serene.stellarutils.io.impl.spark;

import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.spark.json.JSONDataSource;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSource;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

import java.io.IOException;

public class SparkReader implements StellarReader {

    private final SparkSession sparkSession;
    private final String fileFormat;

    public SparkReader(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new NullPointerException("Spark Session was null");
        }
        this.sparkSession = sparkSession;
        this.fileFormat = "json";
    }

    private SparkReader(SparkSession sparkSession, String fileFormat) {
        this.sparkSession = sparkSession;
        this.fileFormat = fileFormat;
    }

    /**
     * Read from parquet file
     *
     * @return  spark graph collection
     */
    public SparkGraphCollection parquet(String path) {
        DataSource parquetDataSource = new ParquetDataSource(path, this.sparkSession);
        return parquetDataSource.getGraphCollection();
    }

    /**
     * Read from json file
     *
     * @return  spark graph collection
     */
    public SparkGraphCollection json(String path) {
        DataSource jsonDataSource = new JSONDataSource(path, this.sparkSession);
        return jsonDataSource.getGraphCollection();
    }

    /**
     * Set file format. Supported formats may vary depending on implementation
     *
     * @param fileFormat file format
     * @return reader object
     */
    @Override
    public StellarReader format(String fileFormat) {
        return new SparkReader(this.sparkSession, fileFormat);
    }

    /**
     * Read graph collection from given path
     *
     * @param path  input path
     * @return graph collection
     */
    @Override
    public StellarGraphCollection getGraphCollection(String path) throws IOException {
        switch (fileFormat.toLowerCase()) {
            case "json":
                return this.json(path);
            case "parquet":
                return this.parquet(path);
            default:
                throw new IOException("Invalid file format: " + fileFormat);
        }
    }
}
