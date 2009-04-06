
package fedora.services.oaiprovider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import fedora.client.FedoraClient;
import fedora.common.PID;
import fedora.server.utilities.StreamUtility;

import proai.SetInfo;
import proai.error.RepositoryException;

/**
 * SetInfo impl that includes setDescription elements for setDiss dissemination,
 * if provided + available.
 */
public class FedoraSetInfo
        implements SetInfo {

    private FedoraClient m_fedora;

    private final PID m_setPID;

    private final String m_setSpec;

    private final String m_setName;

    private final InvocationSpec m_setDiss;

    // if setDiss is null, descriptions don't exist, which is ok
    public FedoraSetInfo(FedoraClient fedora,
                         String setObjectPID,
                         String setSpec,
                         String setName,
                         String setDiss,
                         String setDissInfo) {
        m_fedora = fedora;
        m_setPID = PID.getInstance(setObjectPID);
        m_setSpec = setSpec.replace(' ', '_');
        m_setName = setName;
        if (setDissInfo != null) {
            m_setDiss = InvocationSpec.getInstance(setDiss);
        } else {
            m_setDiss = null;
        }
    }

    public String getSetSpec() {
        return m_setSpec;
    }

    public void write(PrintWriter out) throws RepositoryException {
        out.println("<set>");
        out.println("  <setSpec>" + m_setSpec + "</setSpec>");
        out
                .println("  <setName>" + StreamUtility.enc(m_setName)
                        + "</setName>");
        writeDescriptions(out);
        out.println("</set>");
    }

    private void writeDescriptions(PrintWriter out) throws RepositoryException {
        if (m_setDiss == null) return;
        InputStream in = null;
        try {
            in = m_setDiss.invoke(m_fedora, m_setPID);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            String xml = buf.toString();
            int i = xml.indexOf("<setDescriptions>");
            if (i == -1)
                throw new RepositoryException("Bad set description xml: opening <setDescriptions> not found");
            xml = xml.substring(i + 17);
            i = xml.indexOf("</setDescriptions>");
            if (i == -1)
                throw new RepositoryException("Bad set description xml: closing </setDescrptions> not found");
            out.print(xml.substring(0, i));
        } catch (IOException e) {
            throw new RepositoryException("IO error reading " + m_setDiss, e);
        } finally {
            if (in != null) try {
                in.close();
            } catch (IOException e) {
            }
        }
    }

}
