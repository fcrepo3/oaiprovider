package fedora.services.oaiprovider;

import java.util.*;

import fedora.common.Constants;

import proai.error.*;

public class ITQLQueryFactory implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";

    private String m_oaiItemID;
    private String m_setSpec;
    private String m_setSpecName;
    private String m_setSpecDissType;
    
    public ITQLQueryFactory() {
    }

    public void init(Properties props) throws RepositoryException {
        m_oaiItemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);
        m_setSpec = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC);
        m_setSpecName = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_NAME);
        m_setSpecDissType = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_DISSTYPE);
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

    public Map setInfoQuery() {
        String query = "select $setSpec $setName $setDiss "
                     + "from <#ri> "
                     + "where $set <" + m_setSpec + "> $setSpec "
                     + "and $set <" + m_setSpecName + "> $setName "
                     + "and ($set <" + m_setSpecName + "> $anything "
                          + "or ($set <" + VIEW.DISSEMINATES.uri + "> $setDiss "
                               + "and $setDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + m_setSpecDissType + ">"
                          + ")"
                     + ")";
        Map map = new HashMap();
        map.put("lang", "itql");
        map.put("query", query);
        return map;
    }

/*
get item ids, dissem uris, and sets for all objects in a given format,
even for items that are not in any sets

select $itemID $recordDiss $setSpec
from <#ri>
where $obj <http://www.openarchives.org/OAI/2.0/itemID> $itemID
and $obj <fedora-view:disseminates> $recordDiss
and $recordDiss <fedora-view:disseminationType> <info:fedora/*/oai_dc>
and ($recordDiss <fedora-view:disseminationType> <info:fedora/*/oai_dc>
  or ($obj <fedora-rels-ext:isMemberOf> $set
    and $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec))

*/

/*
get object uris, item ids, modified dates, and sets for all records
in a given format that ARE IN AT LEAST ONE SET

select $obj $itemID $lastMod $setSpec
  from <#ri>
 where $obj <http://www.openarchives.org/OAI/2.0/itemID> $itemID
   and $obj <fedora-view:disseminates> $diss
   and $diss <fedora-view:disseminationType> <info:fedora/*/oai_dc>
   and $diss <fedora-view:lastModifiedDate> $lastMod
   and $obj <fedora-rels-ext:isMemberOf> $set
   and $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec

get the object uris, item ids, and modified dates of all records
in a given format that ARE NOT IN ANY SETS

select $obj $itemID $lastMod
count(select $setSpec 
      from <#ri>
      where $obj <fedora-rels-ext:isMemberOf> $set
      and $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec)
from <#ri>
where $obj <http://www.openarchives.org/OAI/2.0/itemID> $itemID
and $obj <fedora-view:disseminates> $recordDiss
and $recordDiss <fedora-view:disseminationType> <info:fedora/*/oai_dc>
and $recordDiss <fedora-view:lastModifiedDate> $lastMod
having $k0 <http://tucana.org/tucana#occurs>
'0.0'^^<http://www.w3.org/2001/XMLSchema#double>

/*



*/

}
