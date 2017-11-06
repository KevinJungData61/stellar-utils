package au.data61.serene.sereneutils.examples;

import au.data61.serene.sereneutils.core.io.json.JSONDataSource;
import au.data61.serene.sereneutils.core.model.*;
import org.apache.spark.sql.*;

/**
 * Hello world!
 *
 */
public class JSONExample
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils JSON Example")
                .master("local")
                .getOrCreate();

        JSONDataSource dataSource = new JSONDataSource("small-yelp-hin.epgm/", spark);
        GraphCollection gc = dataSource.getGraphCollection();

        gc.getVertices().show(20);
        gc.getEdges().show(20);
        gc.getGraphHeads().show();

    }
}
