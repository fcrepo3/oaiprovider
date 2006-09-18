package fedora.services.oaiprovider;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.SQLProvider;

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
                String setSpec = (String) result.get(setSpecIndex);
                if (setSpec != null) {
                    setSpec = setSpec.replaceAll("\"", "");
                }
                
                String setName = (String) result.get(setNameIndex);
                if (setName != null) {
                    setName = setName.replaceFirst("^\"", "").replaceFirst("\"$", "");
                }
                
                String setDiss = (String) result.get(setDissIndex);
                if (setDiss != null) {
                    setDiss = setDiss.replaceFirst("^<", "").replaceFirst(">$", "");
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
