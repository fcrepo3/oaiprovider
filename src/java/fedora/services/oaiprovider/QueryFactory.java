package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import proai.driver.RemoteIterator;
import fedora.client.FedoraClient;

/**
 * Interface for language-specific query handlers to the Fedora Resource Index.
 * 
 * @author Edwin Shin
 */
public interface QueryFactory {

    public void init(FedoraClient client, FedoraClient queryClient, Properties props);
    
    /**
     * Queries the Fedora Resource Index for the latest last-modified date of 
     * all disseminations that act as metadata for the OAI provider.
     * 
     * @param fedoraMetadataFormats the list of all FedoraMetadataFormats
     * @return date of the latest record
     */
    public Date latestRecordDate(Iterator fedoraMetadataFormats);
    
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
