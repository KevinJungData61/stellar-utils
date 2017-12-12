package sh.serene.stellarutils.model.epgm;

import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.io.DataSink;
import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.io.parquet.ParquetDataSink;
import sh.serene.stellarutils.io.parquet.ParquetDataSource;

public class GraphCollectionReader {

    private SparkSession sparkSession;

    GraphCollectionReader(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new NullPointerException("Spark Session was null");
        }
        this.sparkSession = sparkSession;
    }

    public GraphCollection parquet(String path) {
        DataSource parquetDataSource = new ParquetDataSource(path, this.sparkSession);
        return parquetDataSource.getGraphCollection();
    }

    public GraphCollection json(String path) {
        DataSource jsonDataSource = new JSONDataSource(path, this.sparkSession);
        return jsonDataSource.getGraphCollection();
    }
}
