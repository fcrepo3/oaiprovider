
package fedora.test;

import java.util.Map;

/**
 * General interface for reading fedora configuration parameters.
 * 
 * @author Aaron Birkland
 */
public interface FedoraConfig {

    /** Return all relevant configuration parameters and values */
    public Map<String, String> getParameters();
}
