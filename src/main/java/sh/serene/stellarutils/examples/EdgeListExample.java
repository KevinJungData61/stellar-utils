package sh.serene.stellarutils.examples;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.model.epgm.*;

import java.io.Serializable;
import java.util.*;

/**
 * Example reading from json data source and displaying edge list
 *
 */
public class EdgeListExample {

    public static void main(String[] args) {

        // spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Serene Utils Edge List Example")
                .master("local")
                .getOrCreate();

        // read graph collection
        GraphCollection graphCollection = GraphCollection.read(spark).json("small-yelp-hin.epgm");

        // get edge list of first graph
        Dataset<Tuple2<ElementId,ElementId>> edgeList = graphCollection.get(0).getEdgeList();

        // show
        edgeList.show();

        // edge list as tuples of strings
        edgeList.map((MapFunction<Tuple2<ElementId,ElementId>,Tuple2<String,String>>) tuple -> (
                new Tuple2<>(tuple._1.toString(), tuple._2.toString())
        ), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).show();

    }
}
