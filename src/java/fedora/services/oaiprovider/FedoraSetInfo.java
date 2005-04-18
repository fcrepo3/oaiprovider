package fedora.services.oaiprovider;

import java.io.*;

import proai.*;
import proai.error.*;

/**
 * SetInfo impl that includes setDescription elements for
 * setDiss dissemination, if provided + available.
 */
public class FedoraSetInfo implements SetInfo {

    private String m_setSpec;
    private String m_setName;
    private String m_setDiss;
   
    // if setDiss is null, descriptions don't exist, which is ok
    public FedoraSetInfo(String setSpec, 
                         String setName, 
                         String setDiss) {
        m_setSpec = setSpec;
        m_setName = setName;
        m_setDiss = setDiss;
    }
    
    public String getSetSpec() {
        return m_setSpec;
    }

    public void write(PrintWriter out) throws RepositoryException {
        out.println("<set>");
        out.println("  <setSpec>" + m_setSpec + "</setSpec>");
        out.println("  <setName>" + m_setSpec + "</setName>");
        writeDescriptions(out);
        out.println("</set>");
    }

    private void writeDescriptions(PrintWriter out) throws RepositoryException {
        if (m_setDiss == null) return;
        // TODO: actually retrieve the setDescription(s) from the dissemination, fail gracefully
        out.println("  <setDescription>" + m_setDiss + "</setDescription>");
    }

}
