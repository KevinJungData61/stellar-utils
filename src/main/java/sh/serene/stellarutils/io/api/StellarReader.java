package sh.serene.stellarutils.io.api;

import sh.serene.stellarutils.graph.api.StellarGraphCollection;

import java.io.IOException;

/**
 * Stellar Graph Collection Reader interface
 *
 */
public interface StellarReader {

    /**
     * File format. Supported formats may vary depending on implementation
     *
     * @param fileFormat    file format
     * @return              reader object
     */
    StellarGraphCollection format(String fileFormat) throws IOException;

}
