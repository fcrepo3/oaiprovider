package fedora.services.oaiprovider;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

import org.trippi.TupleIterator;
import org.trippi.RDFFormat;

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
     * Rather than querying for this information, which can be costly,
     * simply return the current date as reported by the remote Fedora server.
     * 
     * @param formats iterator over all FedoraMetadataFormats
     * @return current date according to Fedora
     */
    public Date latestRecordDate(Iterator formats) throws RepositoryException {
        try {
            Date current = m_queryClient.getServerDate();
            logger.info("Current date reported by Fedora is " 
                    + DateUtility.convertDateToString(current));
            return current;
        } catch (IOException e) {
            throw new RepositoryException("Error getting current date from Fedora", e);
        }
    }

    public RemoteIterator listSetInfo() {
        TupleIterator tuples = getTuples(getListSetInfoQuery());
        return new FedoraSetInfoIterator(m_fedora, tuples);
    }
    
    public RemoteIterator listRecords(Date from, 
                                      Date until, 
                                      String dissTypeURI,
                                      String aboutDissTypeURI,
                                      boolean withContent) {

        // Construct and get results of one to three queries, depending on conf

        // Parse and conver the dates once -- they may be used in multiple 
        // queries.  Note that they must be shifted by a millisecond because 
        // the provided dates are inclusive, whereas ITQL date operators are 
        // exclusive
        String afterUTC = null;
        if (from != null) {
            Date afterDate = new Date(from.getTime() + 1);
            afterUTC = DateUtility.convertDateToString(afterDate);
        }

        String beforeUTC = null;
        if (until != null) {
            Date beforeDate = new Date(until.getTime() + 1);
            beforeUTC = DateUtility.convertDateToString(beforeDate);
        }

        // do primary query
        String primaryQuery = getListRecordsPrimaryQuery(afterUTC,
                                                         beforeUTC,
                                                         dissTypeURI);
        File primaryFile = getCSVResults(primaryQuery);

        // do set membership query, if applicable
        File setFile = null;
        if (m_itemSetSpecPath != null && m_itemSetSpecPath.length() > 0) {  // need set membership info
            String setQuery = getListRecordsSetMembershipQuery(afterUTC,
                                                               beforeUTC,
                                                               dissTypeURI);
            setFile = getCSVResults(setQuery);
        }

        // do about query, if applicable
        File aboutFile = null;
        if (aboutDissTypeURI != null && aboutDissTypeURI.length() > 0) { // need about info
            String aboutQuery = getListRecordsAboutQuery(afterUTC,
                                                         beforeUTC,
                                                         dissTypeURI,
                                                         aboutDissTypeURI);
            aboutFile = getCSVResults(aboutQuery);
        }

        // Get a FedoraRecordIterator over the combined results
        // that automatically cleans up the result files when closed

        try {
            ResultCombiner combiner = new ResultCombiner(primaryFile,
                                                         setFile,
                                                         aboutFile,
                                                         true);
            return new CombinerRecordIterator(m_fedora,
                                              dissTypeURI,
                                              aboutDissTypeURI,
                                              combiner);
        } catch (FileNotFoundException e) {
            throw new RepositoryException("Programmer error?  Query result "
                    + "file(s) not found!");
        }
    }

    // FedoraOAIDriver.PROP_DELETED is an optional, object-level (as opposed
    // to dissemination-level) property. If present, use it in place of
    // Fedora state.
    private String getStatePattern() {
        if (m_deleted.equals("")) {
            return "$recordDiss <" + MODEL.STATE + "> $state";
        } else {
            return "$item <" + m_deleted + "> $state";
        }  
    }

    private void appendRecordDissTypePart(String dissTypeURI, StringBuffer out) {
        out.append("and    $recordDiss     <" + VIEW.DISSEMINATION_TYPE + "> <" + dissTypeURI + ">\n");
    }

    private void appendDateParts(String afterUTC, 
                                 String beforeUTC, 
                                 boolean alwaysSelectDate,
                                 StringBuffer out) {
        if (afterUTC == null && beforeUTC == null && !alwaysSelectDate) {
            // we don't have to select the date because 
            // there are no date constraints and the query doesn't ask for it
            return;
        } else {
            out.append("and    $recordDiss     <" + VIEW.LAST_MODIFIED_DATE + "> $date\n");
        }

        // date constraints are optional
        if (afterUTC != null) {
            out.append("and    $date           <" + TUCANA.AFTER + "> '" + afterUTC + "'^^<" + XSD.DATE_TIME + "> in <#xsd>\n");
        }
        if (beforeUTC != null) {
            out.append("and    $date           <" + TUCANA.BEFORE + "> '" + beforeUTC + "'^^<" + XSD.DATE_TIME + "> in <#xsd>\n");
        }
    }

    // ordering is required for the combiner to work
    private void appendOrder(StringBuffer out) {
        out.append("order  by $itemID asc");
    }

    // this is common for all listRecords queries
    private void appendCommonFromWhereAnd(StringBuffer out) {
        out.append("from   <#ri>\n");
        out.append("where  $item           <" + m_oaiItemID + "> $itemID\n");
        out.append("and    $item           <" + VIEW.DISSEMINATES + "> $recordDiss\n");
    }

    private String getListRecordsPrimaryQuery(String afterUTC,
                                              String beforeUTC,
                                              String dissTypeURI) {
        StringBuffer out = new StringBuffer();

        out.append("select $item $itemID $date $state\n");
        appendCommonFromWhereAnd(out);
        out.append("and    " + getStatePattern() + "\n");
        appendRecordDissTypePart(dissTypeURI, out);
        appendDateParts(afterUTC, beforeUTC, true, out);
        appendOrder(out);

        return out.toString();
    }

    private String getListRecordsSetMembershipQuery(String afterUTC,
                                                    String beforeUTC,
                                                    String dissTypeURI) {
        StringBuffer out = new StringBuffer();

        out.append("select $itemID $setSpec\n");
        appendCommonFromWhereAnd(out);
        appendRecordDissTypePart(dissTypeURI, out);
        appendDateParts(afterUTC, beforeUTC, true, out);
        out.append("and    " + m_itemSetSpecPath + "\n");
        appendOrder(out);

        return out.toString();
    }

    private String getListRecordsAboutQuery(String afterUTC,
                                            String beforeUTC,
                                            String dissTypeURI,
                                            String aboutDissTypeURI) {
        StringBuffer out = new StringBuffer();

        out.append("select $itemID\n");
        appendCommonFromWhereAnd(out);
        appendRecordDissTypePart(dissTypeURI, out);
        appendDateParts(afterUTC, beforeUTC, true, out);
        out.append("and $item    <" + VIEW.DISSEMINATES + "> $aboutDiss\n");
        out.append("and $aboutDiss <" + VIEW.DISSEMINATION_TYPE + "> <" + aboutDissTypeURI + ">\n");
        appendOrder(out);

        return out.toString();
    }

    /**
     * Get the results of the given itql tuple query as a temporary CSV file.
     */
    private File getCSVResults(String queryText) throws RepositoryException {

    	logger.debug("getCSVResults() called with query:\n" + queryText);

        Map parameters = new HashMap();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", queryText);

        File tempFile = null;
        OutputStream out = null;
        try {
            tempFile = File.createTempFile("oaiprovider-listrec-tuples", ".csv");
            tempFile.deleteOnExit(); // just in case
            out = new FileOutputStream(tempFile);
        } catch (IOException e) {
            throw new RepositoryException("Error creating temp query result file", e);
        }

        try {
            TupleIterator tuples = m_queryClient.getTuples(parameters);
            logger.info("Saving query results to disk...");
            tuples.toStream(out, RDFFormat.CSV);
            logger.info("Done saving query results");
            return tempFile;
        } catch (Exception e) {
            tempFile.delete();
            throw new RepositoryException("Error getting tuples from Fedora: " +
                                          e.getMessage(), e);
        } finally {
            try { out.close(); } catch (Exception e) { }
        }
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

/*
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

*/
    
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
