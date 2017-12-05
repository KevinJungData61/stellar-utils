package sh.serene.stellarutils.io.csv;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import sh.serene.stellarutils.io.DataSink;
import sh.serene.stellarutils.model.epgm.ElementId;
import sh.serene.stellarutils.model.epgm.GraphCollection;
import sh.serene.stellarutils.model.epgm.GraphHead;
import sh.serene.stellarutils.model.epgm.PropertyGraph;

import java.util.List;

/**
 * CSV Data sink for demonstration purposes. Writes the edge list of each graph in the collection in a separate file.
 *
 */
public class CSVDataSink implements DataSink {

    private final String edgeListPath;

    public CSVDataSink(String edgeListPath) {
        this.edgeListPath = (edgeListPath.charAt(edgeListPath.length()-1) == '/') ? edgeListPath : edgeListPath + '/';
    }

    public void writeGraphCollection(GraphCollection graphCollection) {
        List<GraphHead> graphHeads = graphCollection.getGraphHeads().collectAsList();
        graphHeads.forEach(graphHead -> {
            PropertyGraph propertyGraph = PropertyGraph.fromCollection(graphCollection, graphHead.getId());
            propertyGraph
                    .getEdgeList()
                    .map((MapFunction<Tuple2<ElementId,ElementId>,Tuple2<String,String>>) tup ->
                            (new Tuple2<>(tup._1.toString(), tup._2.toString())),
                            Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                    .write().csv(edgeListPath + graphHead.getId().toString() + ".csv");
        });
    }
}
