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
        Logger.getLogger(FedoraOAIDriver.class.getName());

    private FedoraClient m_fedora;
    private ResultCombiner m_combiner;

    private String m_nextLine;
    
    /**
     * Initialize with combined record query results.
     */
    public CombinerRecordIterator(FedoraClient fedora, ResultCombiner combiner) {
        m_fedora = fedora;
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

    public void close() throws RepositoryException {
        m_combiner.close();
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("CombinerRecordIterator does not support remove().");
    }

    private Record getRecord(String line) throws RepositoryException {
        return null;
        /*
        return new FedoraRecord(m_fedora, 
                                itemID, 
                                recordDiss, 
                                date, 
                                isDeleted, 
                                (String[])sets.toArray(setSpecs), 
                                aboutDiss);
        */
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