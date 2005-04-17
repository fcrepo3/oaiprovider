package fedora.services.oaiprovider;

import java.util.*;

import fedora.common.Constants;

import proai.error.*;

public class ITQLQueryFactory implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";

    private String m_oaiItemID;
    
    public ITQLQueryFactory() {
    }

    public void init(Properties props) throws RepositoryException {
        m_oaiItemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);
    }

    public Map latestRecordDateQuery() {
        String query = "select $date "
                     + "from <#ri> "
                     + "where $object <" + m_oaiItemID + "> $oaiItemID "
                       + "and $object <" + VIEW.DISSEMINATES.uri + "> $diss " 
                       + "and $diss <" + VIEW.LAST_MODIFIED_DATE + "> $date "
                     + "order by $date desc "
                     + "limit 1";
        Map map = new HashMap();
        map.put("lang", "itql");
        map.put("query", query);
        return map;
    }

}
