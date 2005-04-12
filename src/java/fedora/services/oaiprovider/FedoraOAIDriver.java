package fedora.services.oaiprovider;

import java.io.PrintWriter;
import java.util.Date;
import java.util.Properties;

import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 */
public class FedoraOAIDriver implements OAIDriver {

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#init(java.util.Properties)
     */
    public void init(Properties arg0) throws RepositoryException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#write(java.io.PrintWriter)
     */
    public void write(PrintWriter arg0) throws RepositoryException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#getLatestDate()
     */
    public Date getLatestDate() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#listMetadataFormats()
     */
    public RemoteIterator listMetadataFormats() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#listSetInfo()
     */
    public RemoteIterator listSetInfo() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#listRecords(java.util.Date, java.util.Date, java.lang.String, boolean)
     */
    public RemoteIterator listRecords(Date arg0, Date arg1, String arg2, boolean arg3) throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see proai.driver.OAIDriver#close()
     */
    public void close() throws RepositoryException {
        // TODO Auto-generated method stub
        
    }

}
