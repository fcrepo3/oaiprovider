package fedora.services.oaiprovider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

import proai.Record;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.client.FedoraClient;
import fedora.common.Constants;

/**
 * @author Edwin Shin
 */
public class FedoraRecordIterator implements RemoteIterator, Constants {
    private static final Logger logger =
        Logger.getLogger(FedoraOAIDriver.class.getName());
    private FedoraClient m_fedora;
    private TupleIterator m_tuples;

    private Record m_next;
    
    private List m_nextGroup;
    
    /**
     * Initialize with tuples.
     *
     * The tuples should look like:
     * <pre>
     * itemID   recordDiss   date   deleted   setSpec   aboutDiss
     * </pre>
     */
    public FedoraRecordIterator(FedoraClient fedora, TupleIterator tuples) {
        m_fedora = fedora;
        m_tuples = tuples;
        
        m_nextGroup = new ArrayList();
        m_next = getNext();
    }
    
    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#hasNext()
     */
    public boolean hasNext() throws RepositoryException {
        return (m_next != null);
    }

    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#next()
     */
    public Object next() throws RepositoryException {
        try {
            return m_next;
        } finally {
            if (m_next != null) m_next = getNext();
        }
    }

    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#close()
     */
    public void close() throws RepositoryException {
        try {
            m_tuples.close();
        } catch (TrippiException e) {
            throw new RepositoryException("Unable to close tuple iterator", e);
        }
    }

    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#remove()
     */
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("FedoraRecordIterator does not support remove().");
    }
    
    // helper methods
    private Record getNext() throws RepositoryException {
        try {
            List group = getNextGroup();
            if (group.size() == 0) return null;
            Iterator it = group.iterator();
            
            String itemID = null;
            String recordDiss = null;
            String date = null;
            boolean isDeleted = false;
            Set sets = new HashSet();
            String aboutDiss = null;
            String[] values;
            while (it.hasNext() && !isDeleted) {
                values = (String[])it.next();
                if (itemID == null) itemID = values[0];
                if (recordDiss == null) recordDiss = values[1];
                if (date == null) {
                    date = formatDatetime(values[2]);
                }
                isDeleted = !values[3].equals(MODEL.ACTIVE.uri);
                
                // sets and about are optional
                if (values[4] != null && !values[4].equals("")) {
                        sets.add(values[4]);
                }
                
                if (aboutDiss == null && values[5] != null && !values[5].equals("")) {
                    aboutDiss = values[5];
                }
            }
            String[] setSpecs = new String[sets.size()];
            
            return new FedoraRecord(m_fedora, 
                                    itemID, 
                                    recordDiss, 
                                    date, 
                                    isDeleted, 
                                    (String[])sets.toArray(setSpecs), 
                                    aboutDiss);
        } catch (TrippiException e) {
            throw new RepositoryException("Error getting next tuple", e);
        }
    }
    
    /**
     * Return the next group of value[]s that have the same value for the first element.
     * The first element must not be null.
     */
    private List getNextGroup() throws RepositoryException,
                                       TrippiException {
        List group = m_nextGroup;
        m_nextGroup = new ArrayList();
        String commonValue = null;
        if (group.size() > 0) {
            commonValue = ((String[]) group.get(0))[0];
        }
        while (m_tuples.hasNext() && m_nextGroup.size() == 0) {
            String[] values = getValues(m_tuples.next());
            String firstValue = values[0];
            if (firstValue == null) throw new RepositoryException("Not allowed: First value in tuple was null");
            if (commonValue == null) {
                commonValue = firstValue;
            }
            if (firstValue.equals(commonValue)) {
                group.add(values);
            } else {
                m_nextGroup.add(values);
            }
        }
        return group;
    }
    
    private String[] getValues(Map valueMap) throws RepositoryException {
        try {
            String[] names = m_tuples.names();
            String[] values = new String[names.length];
            
            for (int i = 0; i < names.length; i++) {
                values[i] = getString((Node) valueMap.get(names[i]));
            }
            return values;
        } catch (Exception e) {
            throw new RepositoryException("Error getting values from tuple", e);
        }
    }

    private String getString(Node node) throws RepositoryException {
       if (node == null) return null;
       if (node instanceof Literal) {
           return ((Literal) node).getLexicalForm();
       } else if (node instanceof URIReference) {
           return ((URIReference) node).getURI().toString();
       } else {
           throw new RepositoryException("Unhandled node type: " + node.getClass().getName());
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
