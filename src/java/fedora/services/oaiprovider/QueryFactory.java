package fedora.services.oaiprovider;

import java.util.*;
import proai.error.*;

public interface QueryFactory {

    public void init(Properties props) throws RepositoryException;

    public Map latestRecordDateQuery();

    public Map setInfoQuery();

}
