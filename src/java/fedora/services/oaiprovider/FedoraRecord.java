package fedora.services.oaiprovider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import proai.Record;
import proai.error.RepositoryException;
import proai.error.ServerException;
import fedora.client.FedoraClient;

/**
 * @author Edwin Shin
 * @author cwilper@cs.cornell.edu
 */
public class FedoraRecord implements Record {

    public static Logger logger = Logger.getLogger(FedoraRecord.class.getName());

    private static final String _DC_SCHEMALOCATION =
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ "
          + "http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"";
    private static final String _XSI_DECLARATION = 
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";

    private FedoraClient m_fedora;
    private String m_itemID;
    private String m_mdPrefix;

    private String m_sourceInfo;
    
    public FedoraRecord(FedoraClient fedora, 
                        String itemID, 
                        String mdPrefix,
                        String recordDiss, 
                        String date, 
                        boolean deleted, 
                        String[] setSpecs, 
                        String aboutDiss) {

        m_fedora = fedora;

        m_itemID = itemID;
        m_mdPrefix = mdPrefix;

        StringBuffer buf = new StringBuffer();
        buf.append(recordDiss);
        buf.append(" " + aboutDiss);
        buf.append(" " + deleted);
        buf.append(" " + date);
        for (int i = 0; i < setSpecs.length; i++) {
            buf.append(" " + setSpecs[i]);
        }
        m_sourceInfo = buf.toString();
    }

    /* (non-Javadoc)
     * @see proai.Record#getItemID()
     */
    public String getItemID() {
        return m_itemID;
    }

    public String getPrefix() {
        return m_mdPrefix;
    }

    public String getSourceInfo() {
        logger.debug("Returning source info line: " + m_sourceInfo);
        return m_sourceInfo;
    }

/*
    public void write(PrintWriter out) throws ServerException {
    	out.println("<record>");
        writeHeader(out);
        if (!m_deleted) {
            writeMetadata(out);
            if (m_aboutDiss != null && !m_aboutDiss.equals("")) {
                writeAbouts(out);
            }
        }
        out.println("</record>");
    }
    
    private void writeHeader(PrintWriter out) {
        if (m_deleted) {
            out.println("  <header status=\"deleted\">");
        } else {
            out.println("  <header>");
        }
        out.println("    <identifier>" + m_itemID + "</identifier>");
        out.println("    <datestamp>" + m_date + "</datestamp>");
        for (int i = 0; i < m_setSpecs.length; i++) {
            out.println("    <setSpec>" + m_setSpecs[i] + "</setSpec>");
        }
        out.println("  </header>");
    }
    
    private void writeMetadata(PrintWriter out) {
        InputStream in = null;
        try {
            in = m_fedora.get(m_recordDiss, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            String xml = buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
            if ( (m_recordDiss.split("/").length == 3) 
                    && (m_recordDiss.endsWith("/DC")) ) {
                // If it's a DC datastream dissemination, inject the
                // xsi:schemaLocation attribute
                xml = xml.replaceAll("<oai_dc:dc ", 
                                     "<oai_dc:dc " 
                                     + _XSI_DECLARATION + " "
                                     + _DC_SCHEMALOCATION + " ");
            }
            out.println("  <metadata>");
            out.print(xml);
            out.println("  </metadata>");
        } catch (IOException e) {
            throw new RepositoryException("IO error reading " + m_recordDiss, e);
        } finally {
            if (in != null) try { in.close(); } catch (IOException e) { }
        }
    }
    
    private void writeAbouts(PrintWriter out) {
        String aboutWrapperStart = "<abouts>";
        String aboutWrapperEnd = "</abouts>";
        InputStream in = null;
        try {        	
            in = m_fedora.get(m_aboutDiss, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            // strip xml declaration and leading whitespace
            String xml = buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
            int i = xml.indexOf(aboutWrapperStart);
            if (i == -1) throw new RepositoryException("Bad abouts xml: opening " + aboutWrapperStart + " not found");
            xml = xml.substring(i + aboutWrapperStart.length());
            i = xml.lastIndexOf(aboutWrapperEnd);
            if (i == -1) throw new RepositoryException("Bad abouts xml: closing " + aboutWrapperEnd + " not found");
            out.print(xml.substring(0, i));
        } catch (IOException e) {
            throw new RepositoryException("IO error reading aboutDiss " + m_aboutDiss, e);
        } finally {
            if (in != null) try { in.close(); } catch (IOException e) { }
        }
    }

    */
    

/*
<record>
    <header>
      <identifier>oai:arXiv.org:hep-th/9901001</identifier>
      <datestamp>1999-12-25</datestamp>
      <setSpec>physics:hep</setSpec>
      <setSpec>math</setSpec>
    </header>
    <metadata>
     <rfc1807 xmlns=
        "http://info.internet.isi.edu:80/in-notes/rfc/files/rfc1807.txt" 
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
      xsi:schemaLocation=
       "http://info.internet.isi.edu:80/in-notes/rfc/files/rfc1807.txt
        http://www.openarchives.org/OAI/1.1/rfc1807.xsd">
        <bib-version>v2</bib-version>
        <id>hep-th/9901001</id>
        <entry>January 1, 1999</entry>
        <title>Investigations of Radioactivity</title>
        <author>Ernest Rutherford</author>
        <date>March 30, 1999</date>
     </rfc1807>
    </metadata>
    <about>
      <oai_dc:dc 
          xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
          xmlns:dc="http://purl.org/dc/elements/1.1/" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ 
          http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
        <dc:publisher>Los Alamos arXiv</dc:publisher>
        <dc:rights>Metadata may be used without restrictions as long as 
           the oai identifier remains attached to it.</dc:rights>
      </oai_dc:dc>
    </about>
  </record>
 */
}
