package sh.serene.stellarutils.io;

import sh.serene.stellarutils.graph.spark.SparkGraphCollection;

public interface DataSource {

    SparkGraphCollection getGraphCollection();

}
