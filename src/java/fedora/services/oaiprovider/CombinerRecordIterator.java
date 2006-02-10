package fedora.services.oaiprovider;

import org.apache.log4j.Logger;

import proai.Record;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

import fedora.client.FedoraClient;
import fedora.common.Constants;

/**
 * @author cwilper@cs.cornell.edu
 */
public class CombinerRecordIterator implements RemoteIterator, Constants {

    private static final Logger logger =
        Logger.getLogger(CombinerRecordIterator.class.getName());

    private FedoraClient m_fedora;
    private String m_mdPrefix;
    private String m_dissTypeURI;
    private String m_aboutDissTypeURI;
    private ResultCombiner m_combiner;

    private String m_nextLine;
    
    /**
     * Initialize with combined record query results.
     */
    public CombinerRecordIterator(FedoraClient fedora, 
                                  String mdPrefix,
                                  String dissTypeURI,
                                  String aboutDissTypeURI,
                                  ResultCombiner combiner) {
        m_fedora = fedora;
        m_mdPrefix = mdPrefix;
        m_dissTypeURI = dissTypeURI;
        m_aboutDissTypeURI = aboutDissTypeURI;
        m_combiner = combiner;
        m_nextLine = m_combiner.readLine();
    }
    
    public boolean hasNext() {
        return (m_nextLine != null);
    }

    public Object next() throws RepositoryException {
        try {
            return getRecord(m_nextLine);
        } finally {
            if (m_nextLine != null) m_nextLine = m_combiner.readLine();
        }
    }

    public void close() {
        m_combiner.close();
    }

    /**
     * Ensure resources are freed up at garbage collection time.
     */
    protected void finalize() {
        close();
    }
    
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("CombinerRecordIterator does not support remove().");
    }

    /**
     * Construct a record given a line from the combiner.
     *
     * Expected format is:
     *
     * "item","itemID","date","state","hasAbout"[,"setSpec1"[,"setSpec2"[,...]]]
     *
     * For example:
     *
     * info:fedora/nsdl:2051858,oai:nsdl.org:nsdl:10059:nsdl:2051858,2005-09-20T12:50:01,info:fedora/fedora-system:def/model#Active,true,5101,set2
     */
    private Record getRecord(String line) throws RepositoryException {

        String[] parts = line.split(",");

        String itemID = null;
        String recordDissURI = null;
        String utcString = null;
        boolean isDeleted;
        String[] setSpecs = null;
        String aboutDissURI = null;

        // parse the line into values for constructing a FedoraRecord
        try {
            if (parts.length < 5) {
                throw new Exception("Expected at least 5 comma-separated values");
            }

            String pid = parts[0].substring(12); // everything after info:fedora/

            itemID = parts[1];

            recordDissURI = getDissURI(pid, m_dissTypeURI);

            utcString = formatDatetime(parts[2]);

            isDeleted = !parts[3].equals(MODEL.ACTIVE.uri);

            if (parts[4].equals("true")) {
                if (m_aboutDissTypeURI != null) {
                    aboutDissURI = getDissURI(pid, m_aboutDissTypeURI);
                }
            }

            setSpecs = new String[parts.length - 5];
            for (int i = 5; i < parts.length; i++) {
                setSpecs[i - 5] = parts[i];
            }

        } catch (Exception e) {
            throw new RepositoryException("Error parsing combined query "
                    + "results from Fedora: " + e.getMessage() + ".  Input "
                    + "line was: " + line, e);
        }

        // if we got here, all the parameters were parsed correctly
        return new FedoraRecord(m_fedora, 
                                itemID, 
                                m_mdPrefix,
                                recordDissURI, 
                                utcString,
                                isDeleted, 
                                setSpecs,
                                aboutDissURI);
    }

    private String getDissURI(String pid, String dissType) throws Exception {
        try {
            StringBuffer uri = new StringBuffer();
            uri.append("info:fedora/");
            uri.append(pid);
            uri.append(dissType.substring(13)); // starts at the first / after *
            return uri.toString();
        } catch (Throwable th) {
            throw new Exception("Dissemination type string (" + dissType 
                    + ") is too short.");
        }
    }
    
    /**
     * OAI requires second-level precision at most, but Fedora provides 
     * millisecond precision.
     * Fedora only uses UTC dates, so ensure UTC dates are indicated with a 
     * trailing 'Z'.
     * @param datetime
     * @return datetime string such as 2004-01-31T23:11:00Z
     */
    private String formatDatetime(String datetime) {
        StringBuffer sb = new StringBuffer(datetime);
        // length() - 5 b/c at most we're dealing with ".SSSZ"
        int i = sb.indexOf(".", sb.length() - 5);
        if (i != -1) {
            sb.delete(i, sb.length());
        }
        // Kowari's XSD.Datetime isn't timezone aware
        if (sb.charAt(sb.length() - 1) != 'Z') {
            sb.append('Z');
        }
        return sb.toString();
    }

}