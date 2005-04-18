package fedora.services.oaiprovider;

import org.jrdf.graph.*;
import org.trippi.*;

import proai.*;
import proai.driver.*;
import proai.error.*;

public class FedoraSetInfoIterator implements RemoteIterator {

    private TupleIterator m_tuples;

    public FedoraSetInfoIterator(TupleIterator tuples) {
        m_tuples = tuples;


/*
        Map parms = m_queryFactory.setInfoQuery();
        TupleIterator tuples = null;
        try {
            tuples = getTuples(parms);
            while (tuples.hasNext()) {
                Map tuple = tuples.next();
                Literal setSpecLiteral = (Literal) tuple.get("setSpec");
                if (setSpecLiteral == null) throw new RepositoryException("Unexpected: got null setSpec");
                String setSpec = setSpecLiteral.getLexicalForm();
                Literal setNameLiteral = (Literal) tuple.get("setName");
                if (setNameLiteral == null) throw new RepositoryException("Unexpected: got null setName");
                String setName = setNameLiteral.getLexicalForm();
                URIReference setDissReference = (URIReference) tuple.get("setDiss");
                if (setDissReference == null) {
                    System.out.println(setSpec + " -> " + setName);
                } else {
                    String setDiss = setDissReference.getURI().toString();
                    System.out.println(setSpec + " -> " + setName + " -> " + setDiss);
                }
            }
        } catch (Exception e) {
            throw new RepositoryException("Error querying for set information", e);
        } finally {
            if (tuples != null) try { tuples.close(); } catch (Exception e) { }
        }
        return null;
*/
    }

    public boolean hasNext() throws RepositoryException {
        return false;
    }

    public Object next() throws RepositoryException {
        return null;
    }

    public void close() throws RepositoryException {
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("FedoraSetInfoIterator does not support remove().");
    }

}