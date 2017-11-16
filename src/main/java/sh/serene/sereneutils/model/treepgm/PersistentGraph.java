package sh.serene.sereneutils.model.treepgm;

import org.apache.spark.api.java.JavaPairRDD;

public class PersistentGraph {

    final int count;
    final String rootId;
    final JavaPairRDD<String,String> edges;

    public PersistentGraph() {
        this(0, null, null);
    }

    public PersistentGraph(int count, String rootId, JavaPairRDD<String,String> edges) {
        this.count = count;
        this.rootId = rootId;
        this.edges = edges;
    }

    public PersistentGraph assoc(String key) {
        //TODO
        return this;
    }

}
