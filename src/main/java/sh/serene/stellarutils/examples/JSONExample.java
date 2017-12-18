package sh.serene.stellarutils.examples;

import sh.serene.stellarutils.io.gdf.GDFDataSink;
import sh.serene.stellarutils.io.json.JSONDataSink;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;
import sh.serene.stellarutils.entities.VertexCollection;
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
        SparkGraphCollection gc = dataSource.getGraphCollection();

        gc.getVertices().show(20);
        gc.getEdges().show(20);
        gc.getGraphHeads().show();

        gc.getVertices().map((MapFunction<VertexCollection,String>) vertex -> (vertex.getId().toString()), Encoders.STRING()).show();
        gc.getVertices().map((MapFunction<VertexCollection,String>) vertex -> (vertex.getGraphs().get(0).toString()), Encoders.STRING()).show();
        gc.getVertices().map((MapFunction<VertexCollection,Integer>) vertex -> (Integer.parseInt(vertex.getProperty("cool").toString())), Encoders.INT()).show();

        Map<String,Integer> locations = gc.getEdges()
                .filter((FilterFunction<EdgeCollection>) edge -> edge.getLabel().equals("locatedIn"))
                .toJavaRDD().mapToPair((PairFunction<EdgeCollection,String,Integer>) edge -> new Tuple2<>(edge.getProperty("toYelpId").toString(), 1))
                .reduceByKey((Function2<Integer,Integer,Integer>) (a, b) -> (a + b))
                .collectAsMap();
        System.out.println(locations.toString());

        JSONDataSink jsonDataSink = new JSONDataSink("small-yelp-hin-written.epgm/");
        jsonDataSink.writeGraphCollection(gc);

        GDFDataSink gdfDataSink = new GDFDataSink("small-yelp-hin.gdf");
        gdfDataSink.writeGraphCollection(gc);
    }
}
