package sh.serene.sereneutils.examples;

import sh.serene.sereneutils.io.json.JSONDataSource;
import sh.serene.sereneutils.io.parquet.ParquetDataSink;
import sh.serene.sereneutils.io.parquet.ParquetDataSource;
import sh.serene.sereneutils.model.epgm.EPGMEdge;
import sh.serene.sereneutils.model.epgm.EPGMVertex;
import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.sereneutils.model.common.PropertyValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Example for reading and writing to/from Parquet files
 *
 */
public class ParquetExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils JSON Example")
                .master("local")
                .getOrCreate();

        JSONDataSource dataSource = new JSONDataSource("small-yelp-hin.epgm/", spark);
        EPGMGraphCollection gc = dataSource.getGraphCollection();
        gc.getEdges().show();
        gc.getGraphHeads().show();

        ParquetDataSink parquetDataSink = new ParquetDataSink("small-yelp-hin.parquet/");
        parquetDataSink.writeGraphCollection(gc);

        EPGMGraphCollection gcRe = (new ParquetDataSource("small-yelp-hin.parquet/", spark)).getGraphCollection();
        gcRe.getEdges().show(20);
        gcRe.getVertices().show(20);
        gcRe.getGraphHeads().show();
        gcRe.getEdges().map((MapFunction<EPGMEdge,String>) edge -> (edge.getProperty("fromYelpId").toString()), Encoders.STRING()).show();
        gcRe.getVertices().map((MapFunction<EPGMVertex,Boolean>) vertex -> (vertex.getPropertyValue("elite", Boolean.class)), Encoders.BOOLEAN()).show();
        gcRe.getVertices().map((MapFunction<EPGMVertex,String>) vertex -> (vertex.getProperty("elite").toString()), Encoders.STRING()).show();
        gcRe.getVertices().map((MapFunction<EPGMVertex,EPGMVertex>) vertex -> {
            EPGMVertex v = vertex;
            Map<String,PropertyValue> prop = new HashMap<>();
            prop.put("elite", PropertyValue.create(Boolean.valueOf(vertex.getProperty("elite").toString())));
            v.setProperties(prop);
            return v;
        }, Encoders.bean(EPGMVertex.class)).map((MapFunction<EPGMVertex,Boolean>) vertex -> vertex.getPropertyValue("elite", Boolean.class), Encoders.BOOLEAN()).show();

    }

}
