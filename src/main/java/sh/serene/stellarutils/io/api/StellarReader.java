package sh.serene.stellarutils.io.api;

import sh.serene.stellarutils.graph.api.StellarGraphCollection;

import java.io.IOException;

/**
 * Stellar Graph Collection Reader interface
 *
 */
public interface StellarReader {

    /**
     * Set file format. Supported formats may vary depending on implementation
     *
     * @param fileFormat    file format
     * @return              reader object
     */
    StellarReader format(String fileFormat);

    /**
     * Read graph collection from path.
     *
     * @param path  input path
     * @return      graph collection
     */
    StellarGraphCollection getGraphCollection(String path) throws IOException;

    /**
     * Read graph collection from path in json format. This takes precedence over any previous file format setting
     *
     * @param path  input path
     * @return      graph collection
     */
    StellarGraphCollection json(String path);

    /**
     * Read graph colleciton from path in parquet format. This takes precedence over any previous file format setting
     *
     * @param path  input path
     * @return      graph collection
     */
    StellarGraphCollection parquet(String path);

}
