package sh.serene.stellarutils.io.gdf;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import sh.serene.stellarutils.io.DataSink;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;
import sh.serene.stellarutils.entities.VertexCollection;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * GDF data sink. GDF is a format that can be read by Gephi for graph visualisation. This data sink is only to be used
 * for graph collections small enough to fit entirely in memory.
 *
 */
public class GDFDataSink implements DataSink {

    private final String outputPath;

    public GDFDataSink(String outputPath) {
        this.outputPath = outputPath;
    }

    public boolean writeGraphCollection(SparkGraphCollection sparkGraphCollection) {

        try {
            FileWriter writer = new FileWriter(outputPath);
            writer.write("nodedef>name VARCHAR,label VARCHAR,type VARCHAR\n");
            List<String> vertices = sparkGraphCollection
                    .getVertices()
                    .map((MapFunction<VertexCollection,String>) vertex -> (
                            vertex.getId().toString() + "," +
                                    vertex.getLabel() + "," +
                                    vertex.getLabel() + "\n"
                            ), Encoders.STRING())
                    .collectAsList();
            for (String v : vertices) {
                writer.write(v);
            }
            writer.write("edgedef>node1 VARCHAR,node2 VARCHAR,directed BOOLEAN,label VARCHAR,type VARCHAR\n");
            List<String> edges = sparkGraphCollection
                    .getEdges()
                    .map((MapFunction<EdgeCollection,String>) edge -> (
                            edge.getSrc().toString() + "," +
                                    edge.getDst().toString() + "," +
                                    "true," +
                                    edge.getLabel() + "," +
                                    edge.getLabel() + "\n"
                            ), Encoders.STRING())
                    .collectAsList();
            for (String e : edges) {
                writer.write(e);
            }
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }
}
