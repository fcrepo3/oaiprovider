package fedora.services.oaiprovider;

import java.util.Date;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.methods.GetMethod;

import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 */
public class ITQLQueryHandler implements QueryHandler {
    private String m_oaiItemID;
    private String m_fedoraHost;
    private String m_fedoraUser;
    private String m_fedoraPass;
    private String m_riSearch;
    
    public ITQLQueryHandler() {
        // constructor needs to be passed properties,
        // either the driver props file or a Map of the subset of queryhandler props
        m_riSearch = "" + 
                     "?type=tuples" +
                     "&lang=iTQL" +
                     "&format=Simple" +
                     "&query=QUERY_TEXT_OR_URL" +
                     "&template=[TEMPLATE_TEXT_OR_URL (if applicable)]";
    }
    
    /* (non-Javadoc)
     * @see fedora.services.oaiprovider.QueryHandler#getLatestDate()
     */
    public Date getLatestDate() throws RepositoryException {
        String latestDateQuery = 
            "select $date " +
            "from <#ri> " +
            "where " +
                "$object <info:fedora/fedora-system:def/view#disseminates> $diss " +
                "and $object <" + m_oaiItemID + "> $oaiItemID " +
                "and $diss <info:fedora/fedora-system:def/view#lastModifiedDate> $date " +
            "order by $date desc " +
            "limit 1;"; 
        
        HttpClient client = new HttpClient();
        client.getState().setCredentials(
                null,
                m_fedoraHost,
                new UsernamePasswordCredentials(m_fedoraUser, m_fedoraPass)
            );
        GetMethod get = new GetMethod(m_riSearch);
        get.setDoAuthentication(true);
        try {
            int status = client.executeMethod(get);
        } catch (Exception e) {
            throw new RepositoryException("Error querying Fedora", e);
        } finally {
            get.releaseConnection();
        }
        
        
        // TODO Auto-generated method stub
        return null;
    }

}
