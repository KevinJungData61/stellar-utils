package sh.serene.stellarutils.io;

import sh.serene.stellarutils.graph.spark.SparkGraphCollection;

public interface DataSink {

    boolean writeGraphCollection(SparkGraphCollection gc);

}
