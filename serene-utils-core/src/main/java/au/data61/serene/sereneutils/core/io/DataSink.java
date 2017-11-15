package au.data61.serene.sereneutils.core.io;

import au.data61.serene.sereneutils.core.model.epgm.GraphCollection;

public interface DataSink {

    void writeGraphCollection(GraphCollection gc);

}
