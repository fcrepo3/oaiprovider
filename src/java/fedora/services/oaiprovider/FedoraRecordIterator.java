package fedora.services.oaiprovider;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;

import proai.Record;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 */
public class FedoraRecordIterator implements RemoteIterator {
    private FedoraClient m_fedora;
    private TupleIterator m_tuples;

    private Record m_next;
    
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
    }
    
    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#hasNext()
     */
    public boolean hasNext() throws RepositoryException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see proai.driver.RemoteIterator#next()
     */
    public Object next() throws RepositoryException {
        // TODO Auto-generated method stub
        return null;
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

}
