package fedora.services.oaiprovider;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.provider.SQLProvider;
import org.nsdl.mptstore.rdf.Node;

import fedora.client.FedoraClient;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

public class MPTSetInfoIterator implements RemoteIterator {

    private final FedoraClient client;
    private final MPTResultSetsManager results;
    
    private final int setSpecIndex;
    private final int setNameIndex;
    private final int setDissIndex;
    
    public MPTSetInfoIterator(FedoraClient client, SQLProvider queryEngine, DataSource d)  {
        this.client = client;
        try {
            results = new MPTResultSetsManager(d, queryEngine);
        } catch (QueryException e) {
            throw new RepositoryException("Could not generate results query", e);
        }
        
        this.setSpecIndex = queryEngine.getTargets().indexOf("$setSpec");
        if (setSpecIndex == -1) { throw new RuntimeException ("$setSpec not defined");}
        
        this.setNameIndex = queryEngine.getTargets().indexOf("$setName");
        if (setNameIndex == -1) {throw new RuntimeException("$setName not defined");}
        
        this.setDissIndex = queryEngine.getTargets().indexOf("$setDiss");
        if (setDissIndex == -1) {throw new RuntimeException("$setDiss not defined");}
        
    }
    public void close() throws RepositoryException {
        try {
            results.close();
        } catch (SQLException e) {
            throw new RepositoryException("Could not close result set", e);
        }
    }

    public boolean hasNext() throws RepositoryException {
        return results.hasNext();
    }

    public Object next() throws RepositoryException {
        try {
            if (results.hasNext()) {
                List result = results.next();
                String setSpec = null;
                Node setSpecResult = ((Node) result.get(setSpecIndex));
                if (setSpecResult != null) {
                    setSpec = setSpecResult.getValue();
                }
               
                String setName = null;
                Node setNameResult = ((Node) result.get(setNameIndex));
               
                if (setNameResult != null) {
                    setName = setNameResult.getValue();
                }
                
                String setDiss = null;
                Node setDissResult = ((Node) result.get(setDissIndex));
                if (setDissResult != null) {
                    setDiss = setDissResult.getValue();
                }
                return new FedoraSetInfo(client, setSpec, setName, setDiss);
            } else {
                throw new RepositoryException("No more results available\n");
            }
        } catch (SQLException e) {
            throw new RepositoryException("Could not read record result", e);
        }
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("remove() not supported");
    }

}
