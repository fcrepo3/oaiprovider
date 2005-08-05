package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Properties;

import proai.driver.RemoteIterator;
import fedora.client.FedoraClient;

/**
 * Interface for language-specific query handlers to the Fedora Resource Index.
 * 
 * @author Edwin Shin
 */
public interface QueryFactory {

    public void init(FedoraClient client, Properties props);
    
    /**
     * Queries the Fedora Resource Index for the latest last-modified date of 
     * a dissemination for all objects that have an OAI itemID property.
     * 
     * @return date of the latest record
     */
    public Date latestRecordDate();
    
    /**
     * 
     * @return a RemoteIterator of proai.SetInfo objects
     */
    public RemoteIterator listSetInfo();
    
    /**
     * 
     * @param from                  the date (inclusive) of the earliest record 
     *                              to return. Null indicates no lower bound.
     * @param until                 the date (inclusive). Null indicates no 
     *                              upper bound.
     * @param mdPrefixDissType      the dissemination type for a given metadata 
     *                              prefix.
     * @param mdPrefixAboutDissType
     * @param withContent
     * @return                      a RemoteIterator of proai.Record objects
     */
    public RemoteIterator listRecords(Date from, 
                                      Date until, 
                                      String mdPrefixDissType, 
                                      String mdPrefixAboutDissType, 
                                      boolean withContent);
}
