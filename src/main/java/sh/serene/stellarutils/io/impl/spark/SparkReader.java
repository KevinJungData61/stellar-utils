package sh.serene.stellarutils.io.impl.spark;

import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.io.impl.spark.json.JSONDataSource;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSource;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

public class SparkReader {

    private SparkSession sparkSession;

    public SparkReader(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new NullPointerException("Spark Session was null");
        }
        this.sparkSession = sparkSession;
    }

    public SparkGraphCollection parquet(String path) {
        DataSource parquetDataSource = new ParquetDataSource(path, this.sparkSession);
        return parquetDataSource.getGraphCollection();
    }

    public SparkGraphCollection json(String path) {
        DataSource jsonDataSource = new JSONDataSource(path, this.sparkSession);
        return jsonDataSource.getGraphCollection();
    }
}
