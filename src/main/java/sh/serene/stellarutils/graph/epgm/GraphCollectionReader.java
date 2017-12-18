package sh.serene.stellarutils.graph.epgm;

import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.io.parquet.ParquetDataSource;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;

public class GraphCollectionReader {

    private SparkSession sparkSession;

    public GraphCollectionReader(SparkSession sparkSession) {
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
