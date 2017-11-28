package sh.serene.sereneutils.io;

import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;

public interface DataSink {

    void writeGraphCollection(EPGMGraphCollection gc);

}
