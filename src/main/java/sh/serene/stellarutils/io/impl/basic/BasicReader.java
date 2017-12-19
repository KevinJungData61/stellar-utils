package sh.serene.stellarutils.io.impl.basic;

import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.api.StellarReader;

import java.io.IOException;

/**
 * Basic Graph Collection Reader
 *
 */
public class BasicReader implements StellarReader {

    private final String path;

    public BasicReader(String path) {
        this.path = path;
    }

    /**
     * Read from particular file format. Supported formats may vary depending on implementation
     *
     * @param fileFormat file format
     * @return reader object
     */
    @Override
    public StellarGraphCollection format(String fileFormat) throws IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

}
