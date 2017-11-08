package au.data61.serene.sereneutils.examples;

import au.data61.serene.sereneutils.core.io.json.JSONDataSource;
import au.data61.serene.sereneutils.core.model.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

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

        StructType structType = new StructType(new StructField[] {
                new StructField("id", DataTypes.StringType, false, null),
                new StructField("label", DataTypes.StringType, false, null),
                new StructField("cool", DataTypes.StringType, true, null)
        });
        gc.getVertices().map((MapFunction<Vertex,Integer>) vertex -> (vertex.getLabel().equals("user")) ? 1 : 0, Encoders.INT()).show(20);
        gc.getVertices().groupBy("label").count().show(20);
        gc.getVertices().map((MapFunction<Vertex,Row>) vertex -> {
            String id = vertex.getId();
            String label = vertex.getLabel();
            String cool = vertex.getProperties().get("cool");
            return RowFactory.create(id, label, cool);
        }, RowEncoder.apply(structType)).groupBy("cool").count().show();

        gc.getEdges().groupBy("label").count().show();

    }
}
