package fedora.services.oaiprovider;

import java.io.PrintWriter;

import proai.SetInfo;
import proai.error.ServerException;

/**
 * @author Edwin Shin
 */
public class FedoraSetInfo implements SetInfo {
    private String m_setSpec;
    
    public FedoraSetInfo(String setSpec) {
        m_setSpec = setSpec;
    }
    
    /* (non-Javadoc)
     * @see proai.SetInfo#getSetSpec()
     */
    public String getSetSpec() {
        return m_setSpec;
    }

    /* (non-Javadoc)
     * @see proai.Writable#write(java.io.PrintWriter)
     */
    public void write(PrintWriter out) throws ServerException {
        // TODO Auto-generated method stub
        
    }

}
