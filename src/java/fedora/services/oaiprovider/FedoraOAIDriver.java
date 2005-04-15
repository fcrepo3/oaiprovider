package fedora.services.oaiprovider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import proai.driver.impl.RemoteIteratorImpl;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 */
public class FedoraOAIDriver implements OAIDriver {
    private boolean m_initialized;
    private Properties m_properties;
    private QueryHandler m_queryHandler;
    private URL m_identify;
    private String m_fedoraBaseURL;
    private String m_fedoraUser;
    private String m_fedoraPass;
    private String m_itemID;
    private String m_itemSetPath;
    private String m_setSpec;
    private String m_setSpecDissType;
    
    private Collection m_metadataFormats;
    
    public FedoraOAIDriver() {
        m_metadataFormats = new ArrayList();
        
    }
    
    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#init(java.util.Properties)
     */
    public void init(Properties props) throws RepositoryException {
        m_properties = props;
        m_fedoraBaseURL = getProperty("driver.fedora.baseURL"); 
        m_fedoraUser = getProperty("driver.fedora.user"); 
        m_fedoraPass = getProperty("driver.fedora.pass"); 
        try {
            m_identify = new URL(getProperty("driver.fedora.identify"));
        } catch (MalformedURLException e) {
            throw new RepositoryException("Identify property is not a valid URL: " + m_properties.getProperty("identify"), e);
        }
        m_metadataFormats = getMetadataFormats(props);
        m_itemID = getProperty("driver.fedora.itemID");
        m_setSpec = getProperty("driver.fedora.setSpec");
        m_setSpecDissType = getProperty("driver.fedora.setSpec.dissType");
        
        String className = getProperty("driver.fedora.queryHandlerClass");
        try {
            Class queryHandlerClass = Class.forName(className);
            m_queryHandler = (QueryHandler)queryHandlerClass.newInstance();
        } catch (Exception e) {
            throw new RepositoryException("Unable to get an instance of "
                    + className, e);
        }
        
        m_initialized = true;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#write(java.io.PrintWriter)
     */
    public void write(PrintWriter out) throws RepositoryException {
        try {
            BufferedReader reader = new BufferedReader(
                                        new InputStreamReader(m_identify.openStream()));
        
            String line = reader.readLine();
            while (line != null) {
                out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            throw new RepositoryException("Error reading: " + m_identify.toString(), e);
        }
    }

    public Date getLatestDate() throws RepositoryException {
        return m_queryHandler.getLatestDate();
    }

    public RemoteIterator listMetadataFormats() throws RepositoryException {
        return new RemoteIteratorImpl(m_metadataFormats.iterator());
    }

    public RemoteIterator listSetInfo() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteIterator listRecords(Date from, Date until, String mdPrefix, boolean withContent) throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    public void close() throws RepositoryException {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * @param props
     */
    private Collection getMetadataFormats(Properties props) throws RepositoryException {
        String formats[], prefix, namespaceURI, schemaLocation, dissType;
        List list = new ArrayList();
        
        // step through properties, create mf per
        formats = getProperty("driver.fedora.md.formats").split(" ");
        for (int i = 0; i < formats.length; i++) {
            prefix = formats[i];
            namespaceURI = getProperty("driver.fedora.md.format." + prefix + ".loc");
            schemaLocation = getProperty("driver.fedora.md.format." + prefix + ".uri");
            dissType = getProperty("driver.fedora.md.format." + prefix + ".dissType");
            list.add(new FedoraMetadataFormat(prefix, 
                                                     namespaceURI, 
                                                     schemaLocation, 
                                                     dissType));
        }
        return list;
    }
    
    private String getProperty(String key) throws RepositoryException {
        String val = m_properties.getProperty(key);
        if (val == null) {
            throw new RepositoryException("Required property is not set: " + key);
        } else {
            return val;
        }
    }
    

}
