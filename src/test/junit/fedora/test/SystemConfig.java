
package fedora.test;

import java.io.File;
import java.io.FileInputStream;

import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * Gets configuration params necessary to connect to Fedora or its parts.
 * 
 * @author Aaron Birkland
 */
public class SystemConfig
        implements FedoraConfig {

    private static final Logger logger = Logger.getLogger(SystemConfig.class);

    public static final String PARAM_HOST = "fedoraServerHost";

    public static final String PARAM_PORT = "fedoraServerPort";

    public static final String PARAM_SSL_PORT = "fedoraRedirectPort";

    private static final String TRUSTSTORE_LOCATION_PROPERTY =
            "javax.net.ssl.trustStore";

    private static final String TRUSTSTORE_PASSWORD_PROPERTY =
            "javax.net.ssl.trustStorePassword";

    private static final String FEDORA_USERNAME =
            System.getProperty("fedora.username", "fedoraAdmin");

    private static final String FEDORA_PASSWORD =
            System.getProperty("fedora.password", "fedoraAdmin");

    private static final String PARAM_ELEMENT = "param";

    private static final String MODULE_ELEMENT = "module";

    private static final String DATASTORE_ELEMENT = "datastore";

    private static final String NAME_ATTRIBUTE = "name";

    private static final String ID_ATTRIBUTE = "id";

    private static final String ROLE_ATTRIBUTE = "role";

    private static final String VALUE_ATTRIBUTE = "value";

    private static final String CONFIG_FILE_PATH =
            File.separator + "server" + File.separator + "config"
                    + File.separator + "fedora.fcfg";

    private final Element m_container;

    private SystemConfig(Element container) {
        m_container = container;
    }

    /**
     * Get the configuration for a specified module.
     * 
     * @param role
     *        Module role
     * @return Configuration present in the specified module.
     */
    public FedoraConfig getModuleConfig(String role) {
        return getConfigByAttribute(MODULE_ELEMENT, ROLE_ATTRIBUTE, role);
    }

    /**
     * Get the configuration for a specified datastore.
     * 
     * @param id
     *        ID of the datastore.
     * @return Configuration present in the specified datastore.
     */
    public FedoraConfig getDatastoreConfig(String id) {
        return getConfigByAttribute(DATASTORE_ELEMENT, ID_ATTRIBUTE, id);
    }

    private FedoraConfig getConfigByAttribute(String containerName,
                                        String attributeName,
                                        String attributeValue) {
        NodeList containerList =
                m_container.getElementsByTagName(containerName);
        for (int i = 0; i < containerList.getLength(); i++) {
            Element container = (Element) containerList.item(i);
            if (attributeValue.equals(container.getAttribute(attributeName))) {
                return new SystemConfig(container);
            }
        }

        logger.warn("Could not find container " + containerName + " with "
                + attributeName + " = " + attributeValue);
        return new FedoraConfig() {

            public Map<String, String> getParameters() {
                return new HashMap<String, String>();
            }
        };
    }

    /**
     * Get a map of all first-level parameters defined in the fedora config.
     * Does <em>not</em> return configuration embedded in other sections of
     * the fedora.fcfg (i.e. modules, datastores), only the global, top-level
     * parameters.
     */
    public Map<String, String> getParameters() {
        HashMap<String, String> params = new HashMap<String, String>();
        NodeList paramList = m_container.getChildNodes();
        for (int i = 0; i < paramList.getLength(); i++) {
            Node n = paramList.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;

            Element param = (Element) n;

            if (param.getTagName().equals(PARAM_ELEMENT)) {
                params.put(param.getAttribute(NAME_ATTRIBUTE), param
                        .getAttribute(VALUE_ATTRIBUTE));
            }
        }
        return params;
    }

    /**
     * Return the fedora username. The value is <code>fedoraAdmin</code>
     * unless specified otherwise in the system property
     * <code>fedora.username</code>
     * 
     * @return String containing fedora username.
     */
    public String getUser() {
        return FEDORA_USERNAME;
    }

    /**
     * Return the fedora password. The value is <code>fedoraAdmin</code>
     * unless specified otherwise in the system property
     * <code>fedora.password</code>
     * 
     * @return String containing fedora password.
     */
    public String getPass() {
        return FEDORA_PASSWORD;
    }

    /**
     * Read configuration from a sprcific fedora fcfg file.
     * 
     * @param path_to_fedora_fcfg
     *        Path to a valid fedora configuration file
     * @return Populated SystemConfig.
     */
    private static SystemConfig buildFromConfigFile(String path_to_fedora_fcfg) {
        try {
            FileInputStream fcfg = new FileInputStream(path_to_fedora_fcfg);

            DocumentBuilderFactory factory =
                    DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setValidating(false);

            return new SystemConfig(factory.newDocumentBuilder().parse(fcfg)
                    .getDocumentElement());
        } catch (Exception e) {
            throw new RuntimeException("Could not read system configuration", e);
        }
    }

    /**
     * Read configuration present under the given FEDORA_HOME directory. Will
     * look for a file server/config/fedora.fcfg within the specified directory.
     * 
     * @param fedora_home
     *        FEDORA_HOME base directory.
     * @return Populated SystemConfig.
     */
    public static SystemConfig buildFromFedoraHome(String fedora_home) {

        if (System.getProperty(TRUSTSTORE_LOCATION_PROPERTY) == null) {
            System.setProperty(TRUSTSTORE_LOCATION_PROPERTY, fedora_home
                    + "/client/truststore");
        }

        if (System.getProperty(TRUSTSTORE_PASSWORD_PROPERTY) == null) {
            System.setProperty(TRUSTSTORE_PASSWORD_PROPERTY, "tomcat");
        }
        return buildFromConfigFile(fedora_home + CONFIG_FILE_PATH);
    }
}
