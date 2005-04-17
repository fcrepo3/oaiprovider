package fedora.services.oaiprovider;

import java.io.*;
import java.net.*;
import java.util.*;

import fedora.client.Downloader;

import proai.driver.impl.*;
import proai.driver.*;
import proai.error.*;

/**
 * Implementation of the OAIDriver interface for Fedora.
 *
 * @author Edwin Shin, cwilper@cs.cornell.edu
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

    private Downloader m_downloader;
    
    public FedoraOAIDriver() {
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

        try {
            URL baseURL = new URL(m_fedoraBaseURL);
            String host = baseURL.getHost();
            int port = baseURL.getPort();
            if (port < 0) port = baseURL.getDefaultPort();
            m_downloader = new Downloader(host, port, m_fedoraUser, m_fedoraPass);
        } catch (Exception e) {
            throw new RepositoryException("Error parsing baseURL", e);
        }
        
        m_initialized = true;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#write(java.io.PrintWriter)
     */
    public void write(PrintWriter out) throws RepositoryException {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("proai-fedora-identify", ".xml");
            tempFile.deleteOnExit();
            m_downloader.get(m_identify.toString(), new FileOutputStream(tempFile));
            writeStream(new FileInputStream(tempFile), out, m_identify.toString());
        } catch (IOException e) {
            throw new RepositoryException("Unable to write temporary identify.xml", e);
        } finally {
            if (tempFile != null) tempFile.delete(); 
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

    private void writeStream(InputStream in, PrintWriter out, String source) throws RepositoryException {
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
