
package fedora.services.oaiprovider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

import proai.SetInfo;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.client.FedoraClient;

public class FedoraSetInfoIterator
        implements RemoteIterator<SetInfo> {

    private FedoraClient m_fedora;

    private TupleIterator m_tuples;

    private List<String[]> m_nextGroup;

    private SetInfo m_next;

    /**
     * Initialize empty.
     */
    public FedoraSetInfoIterator() {
    }

    /**
     * Initialize with tuples. The tuples should look like:
     * 
     * <pre>
     * "setSpec"    ,"setName"         ,"setDiss"
     * prime        ,Prime             ,null
     * prime        ,Prime             ,info:fedora/demo:SetPrime/SetInfo.xml
     * abovetwo:even,Above Two and Even,null
     * abovetwo:even,Above Two and Even,info:fedora/demo:SetAboveTwoEven/SetInfo.xml
     * abovetwo:odd ,Above Two and Odd ,null
     * abovetwo     ,Above Two         ,null
     * abovetwo     ,Above Two         ,info:fedora/demo:SetAboveTwo/SetInfo.xml
     * </pre>
     */
    public FedoraSetInfoIterator(FedoraClient fedora, TupleIterator tuples)
            throws RepositoryException {
        m_fedora = fedora;
        m_tuples = tuples;
        m_nextGroup = new ArrayList<String[]>();
        m_next = getNext();
    }

    private SetInfo getNext() throws RepositoryException {
        try {
            List<String[]> group = getNextGroup();
            if (group.size() == 0) return null;
            String[] values = (String[]) group.get(group.size() - 1);
            return new FedoraSetInfo(m_fedora,
                                     values[0],
                                     values[1],
                                     values[2],
                                     values[3],
                                     values[4]);
        } catch (TrippiException e) {
            throw new RepositoryException("Error getting next tuple", e);
        }
    }

    /**
     * Return the next group of value[]s that have the same value for the first
     * element. The first element must not be null.
     */
    private List<String[]> getNextGroup() throws RepositoryException,
            TrippiException {
        List<String[]> group = m_nextGroup;
        m_nextGroup = new ArrayList<String[]>();
        String commonValue = null;
        if (group.size() > 0) {
            commonValue = ((String[]) group.get(0))[0];
        }
        while (m_tuples.hasNext() && m_nextGroup.size() == 0) {
            String[] values = getValues(m_tuples.next());
            String firstValue = values[0];
            if (firstValue == null)
                throw new RepositoryException("Not allowed: First value in tuple was null");
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

    @SuppressWarnings("unchecked")
    /* trippi is not generic */
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
            throw new RepositoryException("Unhandled node type: "
                    + node.getClass().getName());
        }
    }

    public boolean hasNext() throws RepositoryException {
        return (m_next != null);
    }

    public SetInfo next() throws RepositoryException {
        try {
            return m_next;
        } finally {
            if (m_next != null) m_next = getNext();
        }
    }

    public void close() throws RepositoryException {
        try {
            if (m_tuples != null) {
                m_tuples.close();
            }
        } catch (TrippiException e) {
            throw new RepositoryException("Unable to close tuple iterator", e);
        }
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("FedoraSetInfoIterator does not support remove().");
    }

}
