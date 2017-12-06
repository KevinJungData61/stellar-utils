package sh.serene.stellarutils.io;

import sh.serene.stellarutils.model.epgm.GraphCollection;

public interface DataSink {

    boolean writeGraphCollection(GraphCollection gc);

}
