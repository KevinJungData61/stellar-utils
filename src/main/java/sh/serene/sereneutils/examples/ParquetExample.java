package sh.serene.sereneutils.examples;

import sh.serene.sereneutils.io.json.JSONDataSource;
import sh.serene.sereneutils.io.parquet.ParquetDataSink;
import sh.serene.sereneutils.io.parquet.ParquetDataSource;
import sh.serene.sereneutils.model.epgm.Edge;
import sh.serene.sereneutils.model.epgm.GraphCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Example for reading and writing to/from Parquet files
 *
 */
public class ParquetExample {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils JSON Example")
                .master("local")
                .getOrCreate();

        JSONDataSource dataSource = new JSONDataSource("small-yelp-hin.epgm/", spark);
        GraphCollection gc = dataSource.getGraphCollection();
        gc.getEdges().show();

        ParquetDataSink parquetDataSink = new ParquetDataSink("small-yelp-hin.parquet/");
        parquetDataSink.writeGraphCollection(gc);

        GraphCollection gcRe = (new ParquetDataSource("small-yelp-hin.parquet/", spark)).getGraphCollection();
        gcRe.getEdges().show(20);
        gcRe.getVertices().show(20);
        gcRe.getGraphHeads().show();
        gcRe.getEdges().map((MapFunction<Edge,String>) edge -> ((String)edge.getProperty("fromYelpId")), Encoders.STRING()).show();

    }

}
