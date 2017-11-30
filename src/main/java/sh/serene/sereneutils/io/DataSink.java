package sh.serene.sereneutils.io;

import sh.serene.sereneutils.model.epgm.GraphCollection;

public interface DataSink {

    void writeGraphCollection(GraphCollection gc);

}
