package fedora.services.oaiprovider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

import fedora.common.Constants;

import org.apache.log4j.Logger;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

import proai.Record;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

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
        System.out.println("********************************");
        System.out.println("* FedoraRecordIterator.getNext()");
        try {
            List group = getNextGroup();
            if (group.size() == 0) return null;
            Iterator it = group.iterator();
            
            String itemID = null;
            String recordDiss = null;
            String date = null;
            boolean deleted = false;
            Set sets = new HashSet();
            String aboutDiss = null;
            
            String[] values;
            while (it.hasNext() && !deleted) {
                System.out.println("* iterating values for " + itemID);
                values = (String[])it.next();
                itemID = values[0];
                recordDiss = values[1];
                date = values[2];
                deleted = !values[3].equals(MODEL.ACTIVE.uri);
                if (values[4] != null && !values[4].equals("")) {
                    sets.add(values[4]);
                }
                
                if (values[5] != null && !values[5].equals("")) {
                    aboutDiss = values[5];
                }
            }
            System.out.println("* final values: ");
            System.out.println("*itemID: " + itemID);
            System.out.println("*recordDiss: " + recordDiss);
            System.out.println("*date: " + date);
            System.out.println("*deleted: " + deleted );
            System.out.println("*setSpec size: " + sets.size());
            System.out.println("*aboutDiss: " + aboutDiss);
            
            String[] setSpecs = new String[sets.size()];
            // FIXME
            // itemID   recordDiss   date   deleted   setSpec   aboutDiss
            return new FedoraRecord(m_fedora, itemID, recordDiss, date, deleted, (String[])sets.toArray(setSpecs), aboutDiss);
            /*
             * FedoraClient fedora, 
                        String itemID, 
                        String recordDiss, 
                        String date, 
                        boolean deleted, 
                        String[] setSpecs, 
                        String aboutDiss) {
             */
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
            System.out.println("=========================================");
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
            Iterator it;
            it = (valueMap.keySet()).iterator();
            while (it.hasNext()) {
                System.out.println("< " +it.next());
            }
            
            it = (valueMap.values()).iterator();
            while (it.hasNext()) {
                System.out.println("> " +getString((Node)it.next()));
            }
            
            for (int i = 0; i < names.length; i++) {
                values[i] = getString((Node) valueMap.get(names[i]));
                System.out.println("? " + names[i] + ": " + values[i]);
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
    
}
