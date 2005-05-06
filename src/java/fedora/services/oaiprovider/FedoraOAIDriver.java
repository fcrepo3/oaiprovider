package fedora.services.oaiprovider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.driver.impl.RemoteIteratorImpl;
import proai.error.RepositoryException;

/**
 * Implementation of the OAIDriver interface for Fedora.
 *
 * @author Edwin Shin, cwilper@cs.cornell.edu
 */
public class FedoraOAIDriver implements OAIDriver {

    private static final Logger logger =
            Logger.getLogger(FedoraOAIDriver.class.getName());

    public static final String NS = "driver.fedora.";

    public static final String PROP_BASEURL               = NS + "baseURL";
    public static final String PROP_USER                  = NS + "user";
    public static final String PROP_PASS                  = NS + "pass";
    public static final String PROP_IDENTIFY              = NS + "identify";
    public static final String PROP_ITEMID                = NS + "itemID";
    public static final String PROP_SETSPEC               = NS + "setSpec";
    public static final String PROP_SETSPEC_NAME          = NS + "setSpec.name";
    public static final String PROP_SETSPEC_DESC_DISSTYPE = NS + "setSpec.desc.dissType";
    public static final String PROP_QUERY_FACTORY         = NS + "queryFactory";
    public static final String PROP_FORMATS               = NS + "md.formats";
    public static final String PROP_FORMAT_START          = NS + "md.format.";
    public static final String PROP_DELETED               = NS + "deleted";
    public static final String PROP_FORMAT_PFX_END        = ".mdPrefix";
    public static final String PROP_FORMAT_LOC_END        = ".loc";
    public static final String PROP_FORMAT_URI_END        = ".uri";
    public static final String PROP_FORMAT_DISSTYPE_END   = ".dissType";
    public static final String PROP_FORMAT_ABOUT_END      = ".about.dissType";
    public static final String PROP_ITEM_SETSPEC_PATH     = NS + "itemSetSpecPath";
    public static final String PROP_VOLATILE              = NS + "volatile";
    
    private QueryFactory m_queryFactory;
    private URL m_identify;
    private String m_fedoraBaseURL;
    private String m_fedoraUser;
    private String m_fedoraPass;
    private String m_itemID;
    private String m_itemSetPath;
    private String m_setSpec;
    private String m_setSpecName;
    private String m_setSpecDissType;
    
    private Map m_metadataFormats;

    private FedoraClient m_fedora;
    
    public FedoraOAIDriver() {}
    
    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Methods from proai.driver.OAIDriver ////////////////
    //////////////////////////////////////////////////////////////////////////

    public void init(Properties props) throws RepositoryException {

        m_fedoraBaseURL   = getRequired(props, PROP_BASEURL);
        if (!m_fedoraBaseURL.endsWith("/")) m_fedoraBaseURL += "/";
        m_fedoraUser      = getRequired(props, PROP_USER); 
        m_fedoraPass      = getRequired(props, PROP_PASS); 
        m_setSpec         = getOptional(props, FedoraOAIDriver.PROP_SETSPEC);

        m_metadataFormats = getMetadataFormats(props);

        try {
            m_identify = new URL(getRequired(props, PROP_IDENTIFY));
        } catch (MalformedURLException e) {
            throw new RepositoryException(
                    "Identify property is not a valid URL: " 
                    + props.getProperty(PROP_IDENTIFY), e);
        }

        String className = getRequired(props, PROP_QUERY_FACTORY);
        try {
            m_fedora = new FedoraClient(m_fedoraBaseURL, m_fedoraUser, m_fedoraPass);
        } catch (Exception e) {
            throw new RepositoryException("Error parsing baseURL", e);
        }
        
        try {
            Class queryFactoryClass = Class.forName(className);
            m_queryFactory = (QueryFactory) queryFactoryClass.newInstance();
            m_queryFactory.init(m_fedora, props);
        } catch (Exception e) {
            throw new RepositoryException("Unable to initialize " + className, e);
        }

        
    }

    public void write(PrintWriter out) throws RepositoryException {
        HttpInputStream in = null;
        try {
            in = m_fedora.get(m_identify.toString(), true);
            writeStream(in, out, m_identify.toString());
        } catch (IOException e) {
            throw new RepositoryException("Error getting identify.xml from " 
                    + m_identify.toString(), e);
        } finally {
            if (in != null) try { in.close(); } catch (Exception e) { }
        }
    }

    // TODO: date for volatile disseminations?
    public Date getLatestDate() throws RepositoryException {
        return m_queryFactory.latestRecordDate();
    }

    public RemoteIterator listMetadataFormats() throws RepositoryException {
        return new RemoteIteratorImpl(m_metadataFormats.values().iterator());
    }

    public RemoteIterator listSetInfo() throws RepositoryException {
        return m_queryFactory.listSetInfo();
    }

    public RemoteIterator listRecords(Date from, 
                                      Date until, 
                                      String mdPrefix, 
                                      boolean withContent) throws RepositoryException {
        if (from != null && until != null && from.after(until)) {
            throw new RepositoryException("from date cannot be later than until date.");
        }
        String mdPrefixDissType = ((FedoraMetadataFormat)m_metadataFormats.get(mdPrefix)).getDissType();
        String mdPrefixAboutDissType = ((FedoraMetadataFormat)m_metadataFormats.get(mdPrefix)).getAbout();
        return m_queryFactory.listRecords(from, until, mdPrefixDissType, mdPrefixAboutDissType, withContent);
    }

    public void close() throws RepositoryException {
        // TODO Auto-generated method stub
        
    }

    //////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Helper Methods ////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    /**
     * @param props
     */
    private Map getMetadataFormats(Properties props) throws RepositoryException {
        String formats[], prefix, namespaceURI, schemaLocation, dissType, about;
        FedoraMetadataFormat mf;
        Map map = new HashMap();
        
        // step through formats, getting appropriate properties for each
        formats = getRequired(props, PROP_FORMATS).split(" ");
        for (int i = 0; i < formats.length; i++) {
            prefix = formats[i];
            namespaceURI   = getRequired(props, PROP_FORMAT_START + prefix + PROP_FORMAT_URI_END);
            schemaLocation = getRequired(props, PROP_FORMAT_START + prefix + PROP_FORMAT_LOC_END);
            dissType       = getRequired(props, PROP_FORMAT_START + prefix + PROP_FORMAT_DISSTYPE_END);
            about          = getOptional(props, PROP_FORMAT_START + prefix + PROP_FORMAT_ABOUT_END);
            
            String otherPrefix = props.getProperty(PROP_FORMAT_START + prefix + PROP_FORMAT_PFX_END);
            if (otherPrefix != null) prefix = otherPrefix;
            mf = new FedoraMetadataFormat(prefix, 
                                          namespaceURI, 
                                          schemaLocation, 
                                          dissType,
                                          about);
            map.put(prefix, mf);
        }
        return map;
    }
    
    protected static String getRequired(Properties props, String key) 
            throws RepositoryException {
        String val = props.getProperty(key);
        if (val == null) {
            throw new RepositoryException("Required property is not set: " + key);
        } else {
            logger.debug(key + " = " + val);
            return val.trim();
        }
    }
    
    /**
     * 
     * @param props
     * @param key
     * @return the value associated with key or the empty String ("")
     */
    protected static String getOptional(Properties props, String key) {
        String val = props.getProperty(key);
        logger.debug(key + " = " + val);
        if (val == null) {
            return "";
        } else {
            return val.trim();
        }
    }

    private void writeStream(InputStream in, 
                             PrintWriter out, 
                             String source) throws RepositoryException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while (line != null) {
                out.println(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RepositoryException("Error reading " + source, e);
        } finally {
            if (reader != null) try { reader.close(); } catch (Exception e) { }
        }
    }
}
