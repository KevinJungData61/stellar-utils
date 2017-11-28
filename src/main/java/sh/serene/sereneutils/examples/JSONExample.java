package sh.serene.sereneutils.examples;

import sh.serene.sereneutils.io.json.JSONDataSink;
import sh.serene.sereneutils.io.json.JSONDataSource;
import sh.serene.sereneutils.model.epgm.EPGMEdge;
import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;
import sh.serene.sereneutils.model.epgm.EPGMVertex;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Map;

/**
 * Example for reading from a JSON Data Source
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
        EPGMGraphCollection gc = dataSource.getGraphCollection();

        gc.getVertices().show(20);
        gc.getEdges().show(20);
        gc.getGraphHeads().show();

        gc.getVertices().map((MapFunction<EPGMVertex,String>) vertex -> (vertex.getId().toString()), Encoders.STRING()).show();
        gc.getVertices().map((MapFunction<EPGMVertex,String>) vertex -> (vertex.getGraphs().get(0).toString()), Encoders.STRING()).show();
        gc.getVertices().map((MapFunction<EPGMVertex,Integer>) vertex -> (Integer.parseInt(vertex.getProperty("cool").toString())), Encoders.INT()).show();

        Map<String,Integer> locations = gc.getEdges()
                .filter((FilterFunction<EPGMEdge>) edge -> edge.getLabel().equals("locatedIn"))
                .toJavaRDD().mapToPair((PairFunction<EPGMEdge,String,Integer>) edge -> new Tuple2<>(edge.getProperty("toYelpId").toString(), 1))
                .reduceByKey((Function2<Integer,Integer,Integer>) (a, b) -> (a + b))
                .collectAsMap();
        System.out.println(locations.toString());

        JSONDataSink jsonDataSink = new JSONDataSink("small-yelp-hin-written.epgm/");
        jsonDataSink.writeGraphCollection(gc);
    }
}
