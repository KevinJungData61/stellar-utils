package sh.serene.stellarutils.io;

import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

public interface DataSource {

    SparkGraphCollection getGraphCollection();

}
