package sh.serene.stellarutils.io.api;

import java.io.IOException;

/**
 * Stellar Graph Collection Writer interface
 *
 */
public interface StellarWriter {

    /**
     * Set file format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat    file format
     * @return              writer object
     */
    StellarWriter format(String fileFormat);

    /**
     * Save graph collection to path.
     *
     * @param path  output path
     * @return      success
     */
    boolean save(String path) throws IOException;

    /**
     * Save graph collection to path in json format. This takes precedence over any previous file format setting.
     *
     * @param path  output path
     * @return      success
     */
    boolean json(String path);

    /**
     * Save graph collection to path in parquet format. This takes precedence over any previous file format setting.
     *
     * @param path  output path
     * @return      success
     */
    boolean parquet(String path);

}
