package sh.serene.stellarutils.io.impl.basic;

import sh.serene.stellarutils.graph.impl.basic.BasicGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;

import java.io.IOException;

public class BasicWriter implements StellarWriter {

    private final BasicGraphCollection graphCollection;

    public BasicWriter(BasicGraphCollection graphCollection, String path) {
        this.graphCollection = graphCollection;
    }

    /**
     * Write with particular file format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat file format
     * @return writer object
     */
    @Override
    public boolean format(String fileFormat) throws IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
