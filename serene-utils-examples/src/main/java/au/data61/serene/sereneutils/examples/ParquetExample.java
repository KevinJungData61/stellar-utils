package au.data61.serene.sereneutils.examples;

import au.data61.serene.sereneutils.core.io.json.JSONDataSource;
import au.data61.serene.sereneutils.core.io.parquet.ParquetDataSink;
import au.data61.serene.sereneutils.core.io.parquet.ParquetDataSource;
import au.data61.serene.sereneutils.core.model.GraphCollection;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class ParquetExample {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils JSON Example")
                .master("local")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.hadoop.parquet.metadata.read.parallelism", "50")
                .getOrCreate();

        JSONDataSource dataSource = new JSONDataSource("small-yelp-hin.epgm/", spark);
        GraphCollection gc = dataSource.getGraphCollection();

        ParquetDataSink parquetDataSink = new ParquetDataSink("small-yelp-hin.parquet/", spark);
        parquetDataSink.writeGraphCollection(gc);

        GraphCollection gcRe = (new ParquetDataSource("small-yelp-hin.parquet/", spark)).getGraphCollection();
        gcRe.getEdges().show(20);
        gcRe.getVertices().show(20);
        gcRe.getGraphHeads().show();

    }

}
