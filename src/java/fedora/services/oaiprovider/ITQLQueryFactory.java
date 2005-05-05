package fedora.services.oaiprovider;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jrdf.graph.Literal;
import org.trippi.TupleIterator;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.common.Constants;
import fedora.server.utilities.DateUtility;

public class ITQLQueryFactory implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";

    private String m_oaiItemID;
    private String m_setSpec;
    private String m_setSpecName;
    private String m_itemSetSpecPath;
    private String m_setSpecDescDissType;
    private String m_deleted;
    private FedoraClient m_fedora;
    
    public ITQLQueryFactory() {
    }

    public void init(FedoraClient client, Properties props) throws RepositoryException {
        m_fedora = client;
        m_oaiItemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);

        m_setSpec = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC);
        if (!m_setSpec.equals("")) {
            m_setSpecName = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_NAME);
            m_itemSetSpecPath = parseItemSetSpecPath(FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH));
            m_setSpecDescDissType = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE);
        }
        m_deleted = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_DELETED);
    }

    public Date latestRecordDate() {
        String query = "select $date\n" +
                       "from <#ri>\n" +
                       "where $object <" + m_oaiItemID + "> $oaiItemID\n" +
                       "and $object <" + VIEW.DISSEMINATES.uri + "> $diss\n" +
                       "and $diss <" + VIEW.LAST_MODIFIED_DATE + "> $date\n" +
                       "order by $date desc\n" +
                       "limit 1";
        Map parms = new HashMap();
        parms.put("lang", QUERY_LANGUAGE);
        parms.put("query", query);

        TupleIterator tuples = getTuples(parms);
        try {
            if (tuples.hasNext()) {
                Literal dateLiteral = (Literal) tuples.next().get("date");
                if (dateLiteral == null) {
                    throw new RepositoryException("A row was returned, but it did not contain a 'date' binding");
                }
                return parseDate(dateLiteral.getLexicalForm());
            } else {
                // no tuples... what to do?
                throw new RepositoryException("No rows returned from query");
            }
        } catch (Exception e) {
            throw new RepositoryException("Error querying for latest changed record date: " + 
                                          e.getMessage(), e);
        } finally {
            if (tuples != null) {
                try { 
                    tuples.close(); 
                } catch (Exception e) { }
            }
        }
    }

    public RemoteIterator listSetInfo() {
        String query = "select $setSpec $setName $setDiss\n" +
                       "from <#ri>\n" +
                       "where $set <" + m_setSpec + "> $setSpec\n" +
                       "and $set <" + m_setSpecName + "> $setName\n" +
                       "and ($set <" + m_setSpecName + "> $anything\n" +
                       "    or ($set <" + VIEW.DISSEMINATES.uri + "> $setDiss\n" +
                       "        and $setDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + m_setSpecDescDissType + ">))";
        Map parms = new HashMap();
        parms.put("lang", QUERY_LANGUAGE);
        parms.put("query", query);
        TupleIterator tuples = getTuples(parms);
        return new FedoraSetInfoIterator(m_fedora, tuples);
    }
    
    public RemoteIterator listRecords(Date from, Date until, String mdPrefixDissType, 
                                String mdPrefixAboutDissType, 
                                boolean withContent) {
        Map parameters = getListRecordsQuery(from, until, mdPrefixDissType, 
                                             mdPrefixAboutDissType, withContent);
        TupleIterator tuples = getTuples(parameters);
        return new FedoraRecordIterator(m_fedora, tuples);
    }
    
    protected Map getListRecordsQuery(Date from, Date until, 
                                      String mdPrefixDissType, 
                                      String mdPrefixAboutDissType, 
                                      boolean withContent) {
        boolean set = !m_setSpec.equals("");
        boolean about = mdPrefixAboutDissType != null && !mdPrefixAboutDissType.equals("");
        boolean custom_delete = !(m_deleted == null || m_deleted.equals(""));
        StringBuffer query = new StringBuffer();
        query.append("select $itemID $recordDiss $date $deleted\n" +
                     "  subquery(\n" +
                     "    select $setSpec\n " +
                     "    from <#ri>\n" +
                     "    where\n");
        if (set) {
            query.append("      $item <" + m_oaiItemID + "> $itemID\n" +
                         "      and $item <" + VIEW.DISSEMINATES.uri + "> $recordDiss\n" +
                         "      and $recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n" +
                         "      and " + m_itemSetSpecPath + "\n");
        } else {
            query.append("      $setSpec <x:x> $setSpec\n");
        }
        query.append(")\n");
        
        query.append("  subquery(\n" +
                     "    select $aboutDiss\n" +
                     "    from <#ri>\n" +
                     "    where\n");
        if (about) {
            query.append("      $item <" + m_oaiItemID + "> $itemID\n" +
                         "      and $item <" + VIEW.DISSEMINATES.uri + "> $recordDiss\n" +
                         "      and $recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n" +
                         "      and $item <" + VIEW.DISSEMINATES + "> $aboutDiss\n" +
                         "      and $aboutDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixAboutDissType + ">");
        } else {
            query.append("      $aboutDiss <x:x> $aboutDiss");
        }
        query.append(")\n");

        query.append("from <#ri>\n" +
                      "where\n" +
                      "  $item <" + m_oaiItemID + "> $itemID\n" +
                      "  and $item <" + VIEW.DISSEMINATES.uri + "> $recordDiss\n" +
                      "  and $recordDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + mdPrefixDissType + ">\n" +
                      "  and $recordDiss <" + VIEW.LAST_MODIFIED_DATE + "> $date\n");
        
        // FedoraOAIDriver.PROP_DELETED is an optional, object-level (as opposed
        // to dissemination-level) property. If present, use it in place of
        // Fedora state.
        // TODO: configurable values for deleted, when property is user-defined?
        if (!custom_delete) {
            query.append("  and $recordDiss <" + MODEL.STATE + "> $deleted\n ");
        } else {
            query.append("  and $item <" + m_deleted + "> $deleted\n ");
        }     
        
        // TODO FIXME
        // From and until dates are optional
        // OAI from/until dates are inclusive boundaries.
        // ITQL before/after are exclusive
        if (from != null) {
            // decrement date by 1 millisecond
            from.setTime(from.getTime() - 1);
            
            query.append("  and $date <" + TUCANA.AFTER + "> '" + 
                         DateUtility.convertDateToString(from) + 
                         "' in <#xsd>\n ");
        }
        if (until != null) {
            // increment date by 1 millisecond
            until.setTime(until.getTime() + 1);
            query.append("  and $date <" + TUCANA.BEFORE + "> '" + 
                         DateUtility.convertDateToString(until) + 
                         "' in <#xsd>\n ");
        }
        
        query.append("  order by $itemID asc");
        Map map = new HashMap();
        map.put("lang", QUERY_LANGUAGE);
        map.put("query", query.toString());
        return map;
    }
    
    private TupleIterator getTuples(Map parameters) throws RepositoryException {
        try {
            return m_fedora.getTuples(parameters);
        } catch (IOException e) {
            throw new RepositoryException("Error getting tuples from Fedora: " +
                                          e.getMessage(), e);
        }
    }

    private Date parseDate(String dateString) throws RepositoryException {
        DateFormat formatter = null;
        
        if (dateString.endsWith("Z")) {
            if (dateString.length() == 20) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            } else if (dateString.length() == 22) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
            } else if (dateString.length() == 23) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
            } else if (dateString.length() == 24) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            }
        } else {
            if (dateString.length() == 19) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            } else if (dateString.length() == 21) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
            } else if (dateString.length() == 22) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS");
            } else if (dateString.length() == 23) {
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            }
        }
        
        try {
            return formatter.parse(dateString);
        } catch (Exception e) {
            throw new RepositoryException("Could not parse date: " + dateString);
        }
    }

    /**
     * 
     * @param itemSetSpecPath
     * @return the setSpec, in the form "$item <$predicate> $setSpec"
     * @throws RepositoryException
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
