package sh.serene.stellarutils.examples;

import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.io.parquet.ParquetDataSink;
import sh.serene.stellarutils.io.parquet.ParquetDataSource;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.VertexCollection;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import sh.serene.stellarutils.entities.PropertyValue;

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
        SparkGraphCollection gc = dataSource.getGraphCollection();
        gc.getEdges().show();
        gc.getGraphHeads().show();

        ParquetDataSink parquetDataSink = new ParquetDataSink("small-yelp-hin.parquet/");
        parquetDataSink.writeGraphCollection(gc);

        SparkGraphCollection gcRe = (new ParquetDataSource("small-yelp-hin.parquet/", spark)).getGraphCollection();
        gcRe.getEdges().show(20);
        gcRe.getVertices().show(20);
        gcRe.getGraphHeads().show();
        gcRe.getEdges().map((MapFunction<EdgeCollection,String>) edge -> (edge.getProperty("fromYelpId").toString()), Encoders.STRING()).show();
        gcRe.getVertices().map((MapFunction<VertexCollection,Boolean>) vertex -> (vertex.getPropertyValue("elite", Boolean.class)), Encoders.BOOLEAN()).show();
        gcRe.getVertices().map((MapFunction<VertexCollection,String>) vertex -> (vertex.getProperty("elite").toString()), Encoders.STRING()).show();
        gcRe.getVertices().map((MapFunction<VertexCollection,VertexCollection>) vertex -> {
            VertexCollection v = vertex;
            Map<String,PropertyValue> prop = new HashMap<>();
            prop.put("elite", PropertyValue.create(Boolean.valueOf(vertex.getProperty("elite").toString())));
            v.setProperties(prop);
            return v;
        }, Encoders.bean(VertexCollection.class)).map((MapFunction<VertexCollection,Boolean>) vertex -> vertex.getPropertyValue("elite", Boolean.class), Encoders.BOOLEAN()).show();

    }

}
