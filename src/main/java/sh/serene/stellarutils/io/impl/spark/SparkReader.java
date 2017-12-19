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
    private final String path;

    public SparkReader(SparkSession sparkSession, String path) {
        if (sparkSession == null) {
            throw new NullPointerException("Spark Session was null");
        }
        this.sparkSession = sparkSession;
        this.path = path;
    }

    /**
     * Read from parquet file
     *
     * @return  spark graph collection
     */
    public SparkGraphCollection parquet() {
        DataSource parquetDataSource = new ParquetDataSource(path, this.sparkSession);
        return parquetDataSource.getGraphCollection();
    }

    /**
     * Read from json file
     *
     * @return  spark graph collection
     */
    public SparkGraphCollection json() {
        DataSource jsonDataSource = new JSONDataSource(path, this.sparkSession);
        return jsonDataSource.getGraphCollection();
    }

    /**
     * Read graph collection from given path
     *
     * @param fileFormat file format
     * @return graph collection
     */
    @Override
    public StellarGraphCollection format(String fileFormat) throws IOException {
        switch (fileFormat.toLowerCase()) {
            case "json":
                return this.json();
            case "parquet":
                return this.parquet();
            default:
                throw new IOException("Invalid file format: " + fileFormat);
        }
    }
}
