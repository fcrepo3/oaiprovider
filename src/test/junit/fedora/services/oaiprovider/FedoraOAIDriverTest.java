
package fedora.services.oaiprovider;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import fedora.common.Constants;
import fedora.common.PID;
import fedora.server.types.gen.RelationshipTuple;
import fedora.services.oaiprovider.test.DisseminationOrigin;
import fedora.services.oaiprovider.test.MPTQueryFactoryPropertiesGenerator;
import fedora.services.oaiprovider.test.MetadataFormatSpec;
import fedora.test.FedoraConfig;
import fedora.test.LiveSystemTests;
import fedora.test.LiveTest;
import fedora.test.SystemConfig;

/**
 * Basic setup and configuration bootstrapping for live fedora OAI driver test
 * cases.
 * <p>
 * Performs all the "icky" parts related to configuring and instantiating a
 * FedoraOAIDriver with the desired properties, ingesting test data, etc.
 * </p>
 * 
 * @author birkland
 */
public abstract class FedoraOAIDriverTest
        extends LiveTest {

    private static final Logger LOG =
            Logger.getLogger(FedoraOAIDriverTest.class);

    public static final String FOXML_TEST_DATA_DIR =
            "src" + File.separator + "test" + File.separator + "foxml"
                    + File.separator;

    private static final String BASE_CONFIG_PATH =
            "src" + File.separator + "test" + File.separator + "config"
                    + File.separator + "base.properties";

    private static final String RI_ROLE =
            "fedora.server.resourceIndex.ResourceIndex";

    private static final String PARAM_RI_LEVEL = "level";

    private static final String PARAM_RI_STORE = "datastore";

    private static final String PARAM_RI_CONNECTOR = "connectorClassName";

    private static final String TRIPPI_CONNECTOR_MPTSTORE =
            "org.trippi.impl.mpt.MPTConnector";

    private static final String TRIPPI_CONNECTOR_MULGARA =
            "org.trippi.impl.mulgara.MulgaraConnector";

    public static final String SET_SPEC_DISSTYPE_DATASTREAM =
            "info:fedora/*/SET_INFO";

    public static final String SET_SPEC_DISSTYPE_SERVICE =
            "info:fedora/*/test:oaiprovider-sdef-getSetInfo/getSetInfo";

    /*
     * The face that the parameter is free-form rather than enumerated values is
     * intentional.
     */
    public static final String MD_DISSTYPE_SERVICE =
            "info:fedora/*/test:oaiprovider-sdef-getMetadata/getMetadata?source=METADATA_INPUT";

    public static final String MD_DISSTYPE_DATASTREAM =
            "info:fedora/*/METADATA";

    public static final String MD_ABOUT_DISSTYPE_SERVICE =
            "info:fedora/*/test:oaiprovider-sdef-getAbout/getAbout";

    public static final String MD_ABOUT_DISSTYPE_DATASTREAM =
            "info:fedora/*/ABOUT";

    public static void ingestIdentify() {
        LiveSystemTests
                .newSession()
                .ingestFrom(new File("src/test/foxml/test_oaiprovider-object-identify.xml"));
    }

    static {
        FedoraConfig riConfig =
                LiveSystemTests
                        .getFedoraConfig()
                        .getModuleConfig("fedora.server.resourceIndex.ResourceIndex");

        if (riConfig != null) {
            Map<String, String> riParams = riConfig.getParameters();

            if (!"1".equals(riParams.get("level"))) {
                LOG.info("ResourceIndex module is off");
            } else {
                if (!"true".equals(riParams.get("syncUpdates"))) {
                    LOG.warn("SyncUpdates is off.  manually flushing equests");
                    LiveSystemTests.FLUSH_UPDATES = true;
                } else {
                    LOG.info("SyncUpdates is on");
                }
            }
        } else {
            LOG.info("ResourceIndex module not configured");
        }
    }

    /**
     * Get a fully configured/initialized Fedora OAI Driver.
     * <p>
     * Will examine the given fedora configuration to determine the appropriate
     * driver parameters, i.e. MPTStore vs Mulgara, correct username/passwords
     * for access, etc.
     * </p>
     * 
     * @return Initialized FedoraOAIDriver
     */
    FedoraOAIDriver getDriver(DisseminationOrigin setInfoOrigin,
                              MetadataFormatSpec... formatSpecs) {

        FedoraOAIDriver driver = new FedoraOAIDriver();

        driver.init(getDriverProperties(setInfoOrigin, formatSpecs));

        return driver;
    }

    SystemConfig getFedoraConfig() {
        return LiveSystemTests.getFedoraConfig();
    }

    /**
     * Determine the correct oaiprovider configuration for the current live
     * fedora configuration.
     * 
     * @return
     */
    private Properties getDriverProperties(DisseminationOrigin setInfoOrigin,
                                           MetadataFormatSpec[] formatSpecs) {

        Properties props = new Properties();
        try {
            /* Basic common properties */
            props.load(new FileInputStream(BASE_CONFIG_PATH));

            /* Load connection-specific properties */
            addConnectionParams(props);

            /* Load QueryFactory-specific properties */
            addQueryFactoryParams(props);

            /* Load metadata format matching properties for each format */
            addMDFormatParams(props, formatSpecs);

            /* Load the setInfo matching property */
            switch (setInfoOrigin) {
                case DATASTREAM:
                    props.put(FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE,
                              SET_SPEC_DISSTYPE_DATASTREAM);
                    break;
                case SERVICE:
                    props.put(FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE,
                              SET_SPEC_DISSTYPE_SERVICE);
                    break;
                default: /* Nothing */
            }

        } catch (Exception e) {
            throw new RuntimeException("Could not load driver properties", e);
        }

        return props;
    }

    private void addConnectionParams(Properties props) {
        SystemConfig config = getFedoraConfig();

        props.put(FedoraOAIDriver.PROP_BASEURL, getSession().getBaseURL());
        props.put(FedoraOAIDriver.PROP_USER, config.getUser());
        props.put(FedoraOAIDriver.PROP_PASS, config.getPass());

        props.put(FedoraOAIDriver.PROP_IDENTIFY, getSession().getBaseURL()
                + "/fedora/get/test:oaiprovider-object-identify/IDENTIFY");
    }

    private void addQueryFactoryParams(Properties props) {
        SystemConfig config = LiveSystemTests.getFedoraConfig();

        FedoraConfig riConfig = config.getModuleConfig(RI_ROLE);

        if (riConfig == null) {
            throw new RuntimeException("No ResourceIndex configuration found");
        }

        Map<String, String> riParams = riConfig.getParameters();

        if (!"1".equals(riParams.get(PARAM_RI_LEVEL))) {
            throw new RuntimeException("ResourceIndex is not on");
        }

        Map<String, String> riStoreParams =
                config.getDatastoreConfig(riParams.get(PARAM_RI_STORE))
                        .getParameters();

        String connector = riStoreParams.get(PARAM_RI_CONNECTOR);

        if (TRIPPI_CONNECTOR_MPTSTORE.equals(connector)) {
            props.put(FedoraOAIDriver.PROP_QUERY_FACTORY, MPTQueryFactory.class
                    .getName());
            props.putAll(MPTQueryFactoryPropertiesGenerator
                    .getProperties(riStoreParams));
        } else if (TRIPPI_CONNECTOR_MULGARA.equals(connector)) {
            props.put(FedoraOAIDriver.PROP_QUERY_FACTORY,
                      ITQLQueryFactory.class.getName());
        } else {
            throw new RuntimeException("Do not know how to test triplestore with connector "
                    + connector);
        }
    }

    private void addMDFormatParams(Properties props,
                                   MetadataFormatSpec[] formatSpecs) {
        String formats = "";
        for (MetadataFormatSpec spec : formatSpecs) {

            formats += " " + spec.getPrefix();
            props.put(FedoraOAIDriver.PROP_FORMAT_START + spec.getPrefix()
                              + FedoraOAIDriver.PROP_FORMAT_LOC_END,
                      "http://www.openarchives.org/OAI/2.0/oai_dc.xsd");
            props.put(FedoraOAIDriver.PROP_FORMAT_START + spec.getPrefix()
                              + FedoraOAIDriver.PROP_FORMAT_URI_END,
                      "http://www.openarchives.org/OAI/2.0/oai_dc/");

            /*
             * Append a suffix to md datastreams for other formats so that they
             * can be distinguished
             */
            String suffix = "";
            if (spec.getPrefix() != "oai_dc") {
                suffix = "_" + spec.getPrefix();
            }

            switch (spec.getMetadataOrigin()) {
                case DATASTREAM:
                    props
                            .put(FedoraOAIDriver.PROP_FORMAT_START
                                         + spec.getPrefix()
                                         + FedoraOAIDriver.PROP_FORMAT_DISSTYPE_END,
                                 MD_DISSTYPE_DATASTREAM + suffix);
                    break;
                case SERVICE:
                    props
                            .put(FedoraOAIDriver.PROP_FORMAT_START
                                         + spec.getPrefix()
                                         + FedoraOAIDriver.PROP_FORMAT_DISSTYPE_END,
                                 MD_DISSTYPE_SERVICE + suffix);
                    break;
                default:
                    throw new RuntimeException("Metadata must be a datastream or service invocation");
            }

            switch (spec.getAboutsOrigin()) {
                case DATASTREAM:
                    props.put(FedoraOAIDriver.PROP_FORMAT_START
                                      + spec.getPrefix()
                                      + FedoraOAIDriver.PROP_FORMAT_ABOUT_END,
                              MD_ABOUT_DISSTYPE_DATASTREAM);
                    break;
                case SERVICE:
                    props.put(FedoraOAIDriver.PROP_FORMAT_START
                                      + spec.getPrefix()
                                      + FedoraOAIDriver.PROP_FORMAT_ABOUT_END,
                              MD_ABOUT_DISSTYPE_SERVICE);
                    break;
                default:
                    break;
            }
        }

        props.put(FedoraOAIDriver.PROP_FORMATS, formats);
    }

    Set<PID> ingestItems(DisseminationOrigin metadataOrigin,
                         DisseminationOrigin metadataAboutsOrigin) {
        return ingestItems(metadataOrigin, metadataAboutsOrigin, -1);
    }

    Set<PID> ingestItems(DisseminationOrigin metadataOrigin,
                         DisseminationOrigin metadataAboutsOrigin,
                         int limit) {
        Set<PID> ingested = new HashSet<PID>();

        for (File member : new File(FOXML_TEST_DATA_DIR).listFiles()) {

            if (ingested.size() == limit) {
                return ingested;
            } else if (member.isFile()
                    && member.getName().contains("item")
                    && member.getName()
                            .contains(getMetadataKey(metadataOrigin))
                    && member.getName()
                            .contains(getAboutKey(metadataAboutsOrigin))) {
                try {
                    Set<PID> newlyIngested;
                    newlyIngested = getSession().ingestFrom(member);

                    for (PID p : newlyIngested) {
                        checkDependencies(p);
                    }
                    ingested.addAll(newlyIngested);
                } catch (Exception e) {
                    throw new RuntimeException("Error ingesting file "
                            + member.getName(), e);
                }
            }
        }

        return ingested;
    }

    private void checkDependencies(PID pid) throws Exception {
        String pidValue = pid.toString();

        if (pidValue.contains("item")) {
            loadDependencies(pid, Constants.MODEL.HAS_MODEL.uri);
        } else if (pidValue.contains("model")) {
            loadDependencies(pid, Constants.MODEL.HAS_SERVICE.uri);
        } else if (pidValue.contains("sdef")) {
            PID bMechPID =
                    PID.getInstance(pid.toString().replaceAll("-sdef-",
                                                              "-smech-"));
            if (!existsInRepository(bMechPID)) {
                getSession().ingestFrom(toFileName(bMechPID));
            }
        }
    }

    private void loadDependencies(PID pid, String relationship)
            throws Exception {
        RelationshipTuple[] rels =
                getClient().getAPIM().getRelationships(pid.toString(),
                                                       relationship);

        if (rels != null && rels.length > 0) {
            for (int i = 0; i < rels.length; i++) {
                PID dependency = PID.getInstance(rels[i].getObject());
                if (!existsInRepository(dependency)
                        && !"fedora-system".equals(dependency.getNamespaceId())) {
                    getSession().ingestFrom(toFileName(dependency));
                    checkDependencies(dependency);
                }
            }
        }
    }

    private File toFileName(PID pid) {
        return new File(FOXML_TEST_DATA_DIR + pid.toString().replace(":", "_")
                + ".xml");
    }

    private boolean existsInRepository(PID pid) throws Exception {
        try {
            getClient().getAPIA().getObjectProfile(pid.toString(), null);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private String getMetadataKey(DisseminationOrigin origin) {
        return getOriginKey("c", origin);
    }

    private String getAboutKey(DisseminationOrigin origin) {
        return getOriginKey("a", origin);
    }

    private String getOriginKey(String base, DisseminationOrigin origin) {
        if (origin == null) {
            return base + ".";
        } else {
            switch (origin) {
                case DATASTREAM:
                    return base + ".d";
                case NONE:
                    return base + ".n";
                case SERVICE:
                    return base + ".s";
                default:
                    throw new RuntimeException("Unknown origin type" + origin);
            }
        }
    }
}