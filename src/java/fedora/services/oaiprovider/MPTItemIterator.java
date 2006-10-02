package fedora.services.oaiprovider;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.provider.SQLProvider;
import org.nsdl.mptstore.rdf.Node;

import fedora.client.FedoraClient;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

public class MPTItemIterator implements RemoteIterator {

    private static final Logger logger =
        Logger.getLogger(MPTItemIterator.class.getName());
    
    private final MPTResultSetsManager results;
    private final String mdPrefix;
    private final FedoraClient client;
    private final String deletedState;
    
    private final int itemIndex;
    private final int itemIDIndex;
    private final int recordDissIndex;
    private final int aboutDissIndex;
    private final int stateIndex;
    private final int dateIndex;
    private final int setSpecIndex;
    private final String recordDiss;
    private final String aboutDiss;
    
    public MPTItemIterator(FedoraClient client, SQLProvider queryEngine, DataSource d, String prefix, String deletedState,
            String recordDiss, String aboutDiss) {
        this.deletedState = deletedState;
        this.client = client;
        this.mdPrefix = prefix;
        this.recordDiss = recordDiss;
        this.aboutDiss = aboutDiss;
        
        try {
            results = new MPTResultSetsManager(d, queryEngine);
        } catch (QueryException e) {
            throw new RepositoryException("Could not generate results query", e);
        }
        
        
        this.itemIDIndex = queryEngine.getTargets().indexOf("$itemID");
        if (itemIDIndex == -1) { throw new RuntimeException ("$itemID not defined");}
        
        this.recordDissIndex = queryEngine.getTargets().indexOf("$recordDiss");
        if (recordDissIndex == -1) { throw new RuntimeException ("$recordDiss not defined");}
        
        this.stateIndex = queryEngine.getTargets().indexOf("$state");
        if (stateIndex == -1) { throw new RuntimeException("stateIndex is not defined");}
        
        this.dateIndex = queryEngine.getTargets().indexOf("$date");
        if (dateIndex == -1) {throw new RuntimeException("dateIndex is not defined");}
        
        this.aboutDissIndex = queryEngine.getTargets().indexOf("$aboutDiss");
        this.setSpecIndex = queryEngine.getTargets().indexOf("$setSpec");
        
        this.itemIndex = queryEngine.getTargets().indexOf("$item");
        if (itemIndex == -1) {throw new RuntimeException("itemIndex is not defined");}
        
    }
    
    public void close() throws RepositoryException {
        try {
            results.close();
        } catch (SQLException e) {
            throw new RepositoryException("Could not close result set", e);
        }
    }

    public boolean hasNext() throws RepositoryException {
        return (results.hasNext());
    }

    public Object next() throws RepositoryException {
        try {
            if (results.hasNext()) {
                List result = results.next();
                
                String pid = (((Node) result.get(itemIndex))).getValue()
                        .replace("info:fedora/", "");
                String itemID = ((Node) result.get(itemIDIndex)).getValue();
                
                String date = formatDate(((Node) result.get(dateIndex)).toString());
                boolean deleted = (((Node) result.get(stateIndex))).toString().equals(deletedState);
            
                String recordDiss = this.recordDiss.replace("*", pid);
                
                String aboutDiss = "";
                if (aboutDissIndex != -1) {
                    aboutDiss = this.aboutDiss.replace("*", pid);
                } 
            
                /* 
                 * Build a set of setSpecs.  This assumes that the results are
                 * grouped by itemID
                 */
                Set setSpecs = new HashSet();
                if (setSpecIndex != -1) {
                    Node setSpecResult = ((Node) result.get(setSpecIndex));
                    if (setSpecResult != null) {
                        setSpecs.add(setSpecResult.getValue());
                    }
                    if (results.peek() != null) {
                        while (results.peek().get(itemIDIndex).toString().equals(itemID)) {
                            List nextEntry = results.next();
                            setSpecResult = (Node) nextEntry.get(setSpecIndex);
                         
                            if (setSpecResult != null) {
                                setSpecs.add(setSpecResult.getValue());
                            }
                            if (results.peek() == null) {break;}
                        }
                    }
                } 
                
                /* All this to create an array[] of the correct type */
                String[] setSpecList = new String[setSpecs.size()];
                int i = 0;
                Iterator setSpecMembers = setSpecs.iterator();
                while (setSpecMembers.hasNext()) {
                    setSpecList[i] = (String) setSpecMembers.next();
                    i++;
                }
                
                

                return new FedoraRecord(this.client, itemID, this.mdPrefix, recordDiss, 
                        date, deleted, (String[]) setSpecList, aboutDiss);
            } else {
                throw new RepositoryException("No more results available\n");
            }
        } catch (SQLException e) {
            logger.error("Could not read recors result", e);
            throw new RepositoryException("Could not read record result", e);
        }
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("remove() not supported");
    }
    /*
     * expects date of the form 
     * "2006-06-14T17:43:22"^^http://www.w3.org/2001/XMLSchema#dateTime
     */
    private String formatDate(String tripleDate) throws RepositoryException {
        
        if (!tripleDate.contains("http://www.w3.org/2001/XMLSchema#dateTime")) {
            throw new RepositoryException("Unknown date format, must be of form "+
                    "'\"YYYY-MM-DDTHH:MM:SS.sss\"^^<http://www.w3.org/2001/XMLSchema#dateTime>" +
                    " but instead was given " + tripleDate);
        }
        
         return tripleDate.replace("\"", "").replace("^^<http://www.w3.org/2001/XMLSchema#dateTime>", "")
            .replaceFirst("\\.[0-9]+Z*", "") + "Z";
    }
}