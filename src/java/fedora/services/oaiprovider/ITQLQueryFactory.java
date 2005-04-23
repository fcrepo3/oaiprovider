package fedora.services.oaiprovider;

import java.util.*;

import fedora.common.Constants;
import fedora.server.utilities.DateUtility;

import proai.error.*;

public class ITQLQueryFactory implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";

    private String m_oaiItemID;
    private String m_setSpec;
    private String m_setSpecName;
    private String m_itemSetSpecPath;
    private String m_setSpecDescDissType;
    private String m_deleted;
    
    public ITQLQueryFactory() {
    }

    public void init(Properties props) throws RepositoryException {
        m_oaiItemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);

        m_setSpec = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC);
        if (!m_setSpec.equals("")) {
            m_setSpecName = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_NAME);
            m_itemSetSpecPath = parseItemSetSpecPath(FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH));
            m_setSpecDescDissType = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE);
        }
        m_deleted = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_DELETED);
    }

    public Map latestRecordDateQuery() {
        String query = "select $date\n"
                     + "from <#ri>\n"
                     + "where $object <" + m_oaiItemID + "> $oaiItemID\n"
                     + "and $object <" + VIEW.DISSEMINATES.uri + "> $diss\n" 
                     + "and $diss <" + VIEW.LAST_MODIFIED_DATE + "> $date\n"
                     + "order by $date desc\n"
                     + "limit 1";
        Map map = new HashMap();
        map.put("lang", "itql");
        map.put("query", query);
        return map;
    }

    public Map setInfoQuery() {
        String query = "select $setSpec $setName $setDiss\n"
                     + "from <#ri>\n"
                     + "where $set <" + m_setSpec + "> $setSpec\n"
                     + "and $set <" + m_setSpecName + "> $setName\n"
                     + "and ($set <" + m_setSpecName + "> $anything\n"
                     + "    or ($set <" + VIEW.DISSEMINATES.uri + "> $setDiss\n"
                     + "       and $setDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + m_setSpecDescDissType + ">))";
        Map map = new HashMap();
        map.put("lang", "itql");
        map.put("query", query);
        return map;
    }
    
    public Map listRecordsQuery(Date from, Date until, String mdPrefixDissType, 
                                String mdPrefixAboutDissType, 
                                boolean withContent) {
        boolean custom_delete = !(m_deleted == null || m_deleted.equals(""));
        boolean set = !m_setSpec.equals("");
        boolean about = mdPrefixAboutDissType != null && !mdPrefixAboutDissType.equals("");
        String setSpec = set ? "$setSpec" : "";
        String aboutDiss = about ? "$aboutDiss" : "";
        
        StringBuffer query = new StringBuffer();
        // base query
        query.append("select $itemID $recordDiss $date $deleted " + setSpec + " " + aboutDiss + "\n " +
                     "from <#ri>\n " +
                     "where $item <" + m_oaiItemID + "> $itemID\n " +
                     "and $item <" + VIEW.DISSEMINATES.uri + "> $recordDiss\n " +
                     "and $recordDiss <" + VIEW.LAST_MODIFIED_DATE + "> $date\n " +
                     "and $recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n ");
        
        // FedoraOAIDriver.PROP_DELETED is an optional, object-level (as opposed
        // to dissemination-level) property. If present, use it in place of
        // Fedora state.
        // TODO: configurable values for deleted, when property is user-defined?
        if (!custom_delete) {
            query.append("and $recordDiss <" + MODEL.STATE + "> $deleted\n ");
        } else {
            query.append("and $item <" + m_deleted + "> $deleted\n ");
        }
        
        // From and until dates are optional
        // OAI from/until dates are inclusive boundaries.
        // ITQL before/after are exclusive
        if (from != null) {
            // decrement date by 1 millisecond
            from.setTime(from.getTime() - 1);
            
            query.append("and $date <" + TUCANA.AFTER + "> '" + 
                         DateUtility.convertDateToString(from) + 
                         "' in <#xsd>\n ");
        }
        if (until != null) {
            // increment date by 1 millisecond
            until.setTime(until.getTime() + 1);
            query.append("and $date <" + TUCANA.BEFORE + "> '" + 
                         DateUtility.convertDateToString(until) + 
                         "' in <#xsd>\n ");
        }
        
        // Set information is optional
        if (set) {
            query.append("and ($recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n " +
                         "     or (" + m_itemSetSpecPath + "))\n ");
        }
        
        // about is optional
        if (about) {
            query.append("and ($recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n " +
                         "     or ($item <" + VIEW.DISSEMINATES + "> $aboutDiss\n " +
                         "         and $aboutDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixAboutDissType + ">))\n ");
        }
        
        query.append("order by $itemID asc ");
        
        Map map = new HashMap();
        map.put("lang", "itql");
        map.put("query", query.toString());
        return map;
    }

/*

select $itemID $recordDiss $date $deleted $setSpec $aboutDiss
 from <#ri>
 where $item <http://www.openarchives.org/OAI/2.0/itemID> $itemID
 and $item <info:fedora/fedora-system:def/view#disseminates> $recordDiss
 and $recordDiss <info:fedora/fedora-system:def/view#lastModifiedDate> $date
 and $recordDiss <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/* /oai_dc>
 and $recordDiss <info:fedora/fedora-system:def/model#state> $deleted
 and $date <http://tucana.org/tucana#after> '1969-12-31T19:00:00.000Z' in <#xsd> 
 and $date <http://tucana.org/tucana#before> '2005-04-21T17:23:26.124Z' in <#xsd>
 and ($recordDiss <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/* /oai_dc>
      or ($item <fedora-rels-ext:isMemberOf> $set and $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec))
 and ($recordDiss <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/* /oai_dc>
      or ($item <info:fedora/fedora-system:def/view#disseminates> $aboutDiss
          and $aboutDiss <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/* /about_oai_dc>))
 order by $itemID asc

get item ids, dissem uris, and sets for all objects in a given format,
even for items that are not in any sets

select $itemID $recordDiss $setSpec
from <#ri>
where $obj <http://www.openarchives.org/OAI/2.0/itemID> $itemID
and $obj <fedora-view:disseminates> $recordDiss 
and $recordDiss <fedora-view:disseminationType> <info:fedora/* /oai_dc>
and ($recordDiss <fedora-view:disseminationType> <info:fedora/* /oai_dc>
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
   and $diss <fedora-view:disseminationType> <info:fedora/* /oai_dc>
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
and $recordDiss <fedora-view:disseminationType> <info:fedora/* /oai_dc>
and $recordDiss <fedora-view:lastModifiedDate> $lastMod
having $k0 <http://tucana.org/tucana#occurs>
'0.0'^^<http://www.w3.org/2001/XMLSchema#double>

*/

    private String parseItemSetSpecPath(String itemSetSpecPath) throws RepositoryException {
        String msg = "Required property, itemSetSpecPath, ";
        String[] path = itemSetSpecPath.split("\\s+");
        if (itemSetSpecPath.indexOf("$item") == -1) {
            throw new RepositoryException(msg + "must include \"$item\"");
        }
        if (itemSetSpecPath.indexOf("$setSpec") == -1) {
            throw new RepositoryException(msg + "must include \"$setSpec\"");
        }
        if ( !itemSetSpecPath.matches("(\\$\\w+\\s+<\\S+>\\s+\\$\\w+\\s*)+") ) {
            throw new RepositoryException(msg + "must be of the form $item <predicate> $setSpec");
        }
        if (path.length == 3 && path[1].equals(m_setSpec)) {
            throw new RepositoryException(msg + "may not use the same predicate as defined in setSpec");
        }
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < path.length; i++) {
            if (i != 0) {
                sb.append(" ");
                if (i % 3 == 0) {
                    sb.append("and ");
                }    
            }
            sb.append(path[i]);
            if (path[i].startsWith("$") && !(path[i].equals("$item") || 
                                             path[i].equals("$set") || 
                                             path[i].equals("$setSpec"))) {
                sb.append(path[i].hashCode());
            }
        }
        return(sb.toString());
    }
}
