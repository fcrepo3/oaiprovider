package fedora.services.oaiprovider;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jrdf.graph.Literal;
import org.trippi.TupleIterator;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.client.FedoraClient;
import fedora.common.Constants;
import fedora.server.utilities.DateUtility;

public class ITQLQueryFactory implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";
    private static final Logger logger =
        Logger.getLogger(FedoraOAIDriver.class.getName());
    
    private String m_oaiItemID;
    private String m_setSpec;
    private String m_setSpecName;
    private String m_itemSetSpecPath;
    private String m_setSpecDescDissType;
    private String m_deleted;
    private boolean m_volatile;
    private FedoraClient m_fedora;
    private FedoraClient m_queryClient;
    
    public ITQLQueryFactory() {}

    public void init(FedoraClient client, FedoraClient queryClient, Properties props) {
        m_fedora = client;
        m_queryClient = queryClient;
        m_oaiItemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);

        m_setSpec = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC);
        if (!m_setSpec.equals("")) {
            m_setSpecName = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_NAME);
            m_itemSetSpecPath = parseItemSetSpecPath(FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH));
            m_setSpecDescDissType = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE);
        }
        
        m_deleted = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_DELETED);
        m_volatile = !(FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_VOLATILE).equals(""));
    }

    /**
     * Queries the Fedora Resource Index for the latest last-modified date of 
     * all disseminations that act as metadata for the OAI provider.
     * 
     * @param fedoraMetadataFormats the list of all FedoraMetadataFormats
     * @return date of the latest record
     */
    public Date latestRecordDate(Iterator formats) {
        Date latest = null;
        while (formats.hasNext()) {
            FedoraMetadataFormat format = (FedoraMetadataFormat) formats.next();
            Date date = latestRecordDate(format.getDissType());
            if (date == null) {
                logger.info("Format '" + format.getPrefix() + "' had at least "
                        + "one volatile representation; will force update");
                return new Date();
            } else {
                if (date.getTime() == 0) {
                    logger.info("No records were found in format '" 
                            + format.getPrefix() + "'");
                } else {
                    logger.info("Latest date for records of format '" 
                            + format.getPrefix() + "' was " 
                            + DateUtility.convertDateToString(date));
                }
                if (latest == null) {
                    latest = date;
                } else {
                    if (date.getTime() > latest.getTime()) {
                        latest = date;
                    }
                }
            }
        }
        logger.info("Latest date of all records was " 
                + DateUtility.convertDateToString(latest));
        return latest;
    }

    /**
     * Query to find the latest-dated record of the format indicated by
     * the given dissType, or return null if at least one of the matching
     * records is volatile.
     */
    private Date latestRecordDate(String dissType) {
        TupleIterator tuples = getTuples(getLatestRecordDateQuery(dissType));
        try {
            if (tuples.hasNext()) {
                Map row = tuples.next();
                if (m_volatile && row.get("volatile") != null && !row.get("volatile").equals("")) {
                    // FIXME get current date from the repository
                    return new Date();
                } else {
                    Literal dateLiteral = (Literal) row.get("date");
                    if (dateLiteral == null) {
                        throw new RepositoryException("A row was returned, but it did not contain a 'date' binding");
                    }
                    return DateUtility.parseDateAsUTC(dateLiteral.getLexicalForm());
                }
            } else {
                return new Date(0); // no records found for this format.
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

    private String getLatestRecordDateQuery(String dissType) {
        String query = "select $date\n" +
                       "  subquery(\n" +
                       "    select $volatile\n" +
                       "    from <#ri>\n" +
                       "    where $x <" + m_oaiItemID + "> $y\n" +
                       "      and $x <" + VIEW.DISSEMINATES + "> $z\n" +
                       "      and $z <" + VIEW.DISSEMINATION_TYPE.uri + "> <" + dissType + ">\n" +
                       "      and $z <" + VIEW.IS_VOLATILE.uri + "> $volatile\n" +
                       "      and $volatile <" + TUCANA.IS.uri + "> 'true'\n" +
                       "  )\n" +
                       "from <#ri>\n" +
                       "where $object <" + m_oaiItemID + "> $oaiItemID\n" +
                       "  and $object <" + VIEW.DISSEMINATES + "> $diss\n" +
                       "  and $diss <" + VIEW.DISSEMINATION_TYPE.uri + "> <" + dissType + ">\n" +
                       "  and $diss <" + VIEW.LAST_MODIFIED_DATE.uri + "> $date\n" +
                       "  order by $date desc\n" +
                       "  limit 1\n";
        return query;
    }


    public RemoteIterator listSetInfo() {
        TupleIterator tuples = getTuples(getListSetInfoQuery());
        return new FedoraSetInfoIterator(m_fedora, tuples);
    }
    
    public RemoteIterator listRecords(Date from, Date until, String mdPrefixDissType, 
                                String mdPrefixAboutDissType, 
                                boolean withContent) {
        String query = getListRecordsQuery(from, until, mdPrefixDissType, 
                                             mdPrefixAboutDissType, withContent);
        TupleIterator tuples = getTuples(query);
        return new FedoraRecordIterator(m_fedora, tuples);
    }
    
    /**
     * Builds the iTQL query for record listings.
     * 
     * <pre>
select $itemID $recordDiss $date $deleted
  subquery(
    select $setSpec
    from &lt;#ri&gt;
    where $setSpec &lt;x:x&gt; &lt;x:x&gt;
  )
  subquery(
    select $aboutDiss
    from &lt;#ri&gt;
    where $item &lt;http://www.openarchives.org/OAI/2.0/itemID&gt; $itemID
      and $item &lt;fedora-view:disseminates&gt; $recordDiss
      and $recordDiss &lt;fedora-view:disseminationType&gt; &lt;info:fedora&#8260;*&#8260;oai_dc&gt;
      and $item &lt;fedora-view:disseminates&gt; $aboutDiss
      and $aboutDiss &lt;fedora-view:disseminationType&gt; &lt;info:fedora&#8260;*&#8260;about_oai_dc&gt;
  )

from &lt;#ri&gt;
where $item &lt;http://www.openarchives.org/OAI/2.0/itemID&gt; $itemID
  and $item &lt;fedora-view:disseminates&gt; $recordDiss
  and $recordDiss &lt;fedora-view:lastModifiedDate&gt; $date
  and $recordDiss &lt;fedora-model:state&gt; $deleted
  and $recordDiss &lt;fedora-view:disseminationType&gt; &lt;info:fedora&#8260;*&#8260;oai_dc&gt;
  order by $itemID asc;
     * </pre>
     * 
     * @param from
     * @param until
     * @param mdPrefixDissType
     * @param mdPrefixAboutDissType
     * @param withContent
     * @return the iTQL query for ListRecords
     */
    protected String getListRecordsQuery(Date from, Date until, 
                                      String mdPrefixDissType, 
                                      String mdPrefixAboutDissType, 
                                      boolean withContent) {
        boolean set = !m_setSpec.equals("");
        boolean about = mdPrefixAboutDissType != null && !mdPrefixAboutDissType.equals("");
        StringBuffer query = new StringBuffer();
        query.append("select $item $recordDissType $itemID $date $deleted\n" +
                     "  subquery(\n" +
                     "    select $setSpec\n " +
                     "    from <#ri>\n" +
                     "    where\n");
        if (set) {
            query.append("      $item <" + m_oaiItemID + "> $itemID\n" +
                         "      and $item <" + VIEW.DISSEMINATES + "> $recordDiss\n" +
                         "      and $recordDiss <" + VIEW.DISSEMINATION_TYPE.uri + "> <" + mdPrefixDissType + ">\n" +
                         "      and " + m_itemSetSpecPath + "\n");
        } else {
            // we don't want to match anything
            query.append("      $setSpec <test:noMatch> <test:noMatch>\n");
        }
        query.append("  )\n");
        
        query.append("  subquery(\n" +
                     "    select $aboutDissType\n" +
                     "    from <#ri>\n" +
                     "    where\n");
        if (about) {
            query.append("      $item <" + m_oaiItemID + "> $itemID\n" +
                         "      and $item <" + VIEW.DISSEMINATES + "> $recordDiss\n" +
                         "      and $recordDiss <" + VIEW.DISSEMINATION_TYPE.uri + "> <" + mdPrefixDissType + ">\n" +
                         "      and $item <" + VIEW.DISSEMINATES + "> $aboutDiss\n" +
                         "      and $aboutDiss <" + VIEW.DISSEMINATION_TYPE.uri + "> $aboutDissType\n" +
						 "      and $aboutDissType <" + TUCANA.IS.uri + "> <" + mdPrefixAboutDissType + ">");
        } else {
            // we don't want to match anything
            query.append("      $aboutDissType <test:noMatch> <test:noMatch>");
        }
        query.append(")\n");

        query.append("from <#ri>\n" +
                      "where\n" +
                      "  $item <" + m_oaiItemID + "> $itemID\n" +
                      "  and $item <" + VIEW.DISSEMINATES + "> $recordDiss\n" +
                      "  and $recordDiss <" + VIEW.DISSEMINATION_TYPE.uri + "> $recordDissType\n" +
                      "  and $recordDissType <" + TUCANA.IS.uri + "> <" + mdPrefixDissType + ">\n" +
                      "  and $recordDiss <" + VIEW.LAST_MODIFIED_DATE.uri + "> $date\n");
        
        // FedoraOAIDriver.PROP_DELETED is an optional, object-level (as opposed
        // to dissemination-level) property. If present, use it in place of
        // Fedora state.
        if (m_deleted.equals("")) {
            query.append("  and $recordDiss <" + MODEL.STATE + "> $deleted\n");
        } else {
            query.append("  and $item <" + m_deleted + "> $deleted\n");
        }     
        
        // From and until dates are optional
        // OAI from/until dates are inclusive boundaries.
        // iTQL before/after are exclusive
        if (from != null) {
            // decrement date by 1 millisecond
            from.setTime(from.getTime() - 1);
            
            query.append("  and $date <" + TUCANA.AFTER + "> '" + 
                         DateUtility.convertDateToString(from) + 
                         "'^^<" + XSD.DATE_TIME + "> in <#xsd>\n");
        }
        if (until != null) {
            // increment date by 1 millisecond
            until.setTime(until.getTime() + 1);
            query.append("  and $date <" + TUCANA.BEFORE + "> '" + 
                         DateUtility.convertDateToString(until) + 
                         "'^^<" + XSD.DATE_TIME + "> in <#xsd>\n");
        }
        
        query.append("  order by $itemID asc\n");
        return query.toString();
    }
    
    protected String getListSetInfoQuery() {
        boolean setDesc = m_setSpecDescDissType != null && !m_setSpecDescDissType.equals("");
        StringBuffer query = new StringBuffer();
        query.append("select $setSpec $setName\n" +
        		     "  subquery(" +
        		     "    select $setDiss\n" +
        		     "	  from <#ri>\n" +
        		     "      where\n");
        if (setDesc) {
            query.append("      $set <" + m_setSpec + "> $setSpec\n" +
                         "      and $set <" + m_setSpecName + "> $setName\n" +
	        		     "      and $set <" + VIEW.DISSEMINATES + "> $setDiss\n" +
	        		     "      and $setDiss <" + VIEW.DISSEMINATION_TYPE.uri + "> <" + m_setSpecDescDissType + ">");
        } else {
            // we don't want to match anything
            query.append("      $setDiss <test:noMatch> <test:noMatch>\n");
        }
        query.append(")\n");
        query.append("from <#ri>" +
        		     "where $set <" + m_setSpec + "> $setSpec\n" +
                     "and $set <" + m_setSpecName + "> $setName");
        return query.toString();
    }
    
    private TupleIterator getTuples(String query) throws RepositoryException {
    	logger.debug("getTuples() called with query: ");
    	logger.debug(query);
        Map parameters = new HashMap();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", query);
        
        try {
            return m_queryClient.getTuples(parameters);
        } catch (IOException e) {
            throw new RepositoryException("Error getting tuples from Fedora: " +
                                          e.getMessage(), e);
        }
    }

    /**
     * 
     * @param itemSetSpecPath
     * @return the setSpec, in the form "$item <$predicate> $setSpec"
     * @throws RepositoryException
     */
    protected String parseItemSetSpecPath(String itemSetSpecPath) throws RepositoryException {
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
