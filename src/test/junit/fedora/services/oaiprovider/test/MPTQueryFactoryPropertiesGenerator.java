
package fedora.services.oaiprovider.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static fedora.services.oaiprovider.MPTQueryFactory.*;

/**
 * Generates FedoraOAIDriver-compliant property name/value pairs based on
 * fedora.fcfg values.
 * 
 * @author birkland
 */
public class MPTQueryFactoryPropertiesGenerator {

    private static List<String> fedoraParams =
            Arrays.asList("jdbcDriver",
                          "ddlGenerator",
                          "jdbcURL",
                          "username",
                          "password",
                          "backslashIsEscape");

    private static List<String> driverConfigParams =
            Arrays.asList(PROP_DB_DRIVER,
                          PROP_DDL,
                          PROP_URL,
                          PROP_USERNAME,
                          PROP_PASSWD,
                          PROP_BACKSLASH_ESCAPE);

    public static Properties getProperties(Map<String, String> fedoraParamValues) {
        Properties p = new Properties();

        for (int i = 0; i < fedoraParams.size(); i++) {
            p.put(driverConfigParams.get(i), fedoraParamValues.get(fedoraParams
                    .get(i)));
        }

        /* I think trippi must configure/assume these itself... */
        p.put("driver.fedora.mpt.db.map", "tmap");
        p.put("driver.fedora.mpt.db.prefix", "t");
        return p;
    }
}
