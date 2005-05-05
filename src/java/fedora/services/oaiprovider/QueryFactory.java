package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Properties;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

public interface QueryFactory {

    public void init(FedoraClient client, Properties props) throws RepositoryException;

    public Date latestRecordDate();
    
    public RemoteIterator listSetInfo();
    
    public RemoteIterator listRecords(Date from, Date until, String mdPrefixDissType, String mdPrefixAboutDissType, boolean withContent);

}
