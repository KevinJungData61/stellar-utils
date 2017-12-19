package sh.serene.stellarutils.io.api;

import java.io.IOException;

/**
 * Stellar Graph Collection Writer interface
 *
 */
public interface StellarWriter {

    /**
     * File format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat    file format
     * @return              writer object
     */
    boolean format(String fileFormat) throws IOException;

}
