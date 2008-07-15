
package fedora.services.oaiprovider;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

import org.trippi.TupleIterator;
import org.trippi.RDFFormat;

import proai.MetadataFormat;
import proai.SetInfo;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

import fedora.client.FedoraClient;
import fedora.common.Constants;
import fedora.server.utilities.DateUtility;

public class ITQLQueryFactory
        implements QueryFactory, Constants {

    private static final String QUERY_LANGUAGE = "itql";

    private static final Logger logger =
            Logger.getLogger(FedoraOAIDriver.class.getName());

    private String m_oaiItemID;

    private String m_setSpec;

    private String m_setSpecName;

    private String m_itemSetSpecPath;

    private String m_deleted;

    private FedoraClient m_fedora;

    private FedoraClient m_queryClient;

    public ITQLQueryFactory() {
    }

    public void init(FedoraClient client,
                     FedoraClient queryClient,
                     Properties props) {
        m_fedora = client;
        m_queryClient = queryClient;
        m_oaiItemID =
                FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);

        m_setSpec =
                FedoraOAIDriver
                        .getOptional(props, FedoraOAIDriver.PROP_SETSPEC);
        if (!m_setSpec.equals("")) {
            m_setSpecName =
                    FedoraOAIDriver
                            .getRequired(props,
                                         FedoraOAIDriver.PROP_SETSPEC_NAME);
            m_itemSetSpecPath =
                    parseItemSetSpecPath(FedoraOAIDriver
                            .getRequired(props,
                                         FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH));
        }

        m_deleted =
                FedoraOAIDriver
                        .getOptional(props, FedoraOAIDriver.PROP_DELETED);
    }

    /**
     * Rather than querying for this information, which can be costly, simply
     * return the current date.
     * 
     * @param formats
     *        iterator over all FedoraMetadataFormats
     * @return current date according to Fedora
     */
    public Date latestRecordDate(Iterator<? extends MetadataFormat> formats)
            throws RepositoryException {
        Date current = new Date();

        logger.info("Current date reported by Fedora is "
                + DateUtility.convertDateToString(current));
        return current;
    }

    public RemoteIterator<SetInfo> listSetInfo(InvocationSpec setInfoSpec) {
        if (m_itemSetSpecPath == null) {
            // return empty iterator if sets not configured
            return new FedoraSetInfoIterator();
        } else {
            TupleIterator tuples = getTuples(getListSetInfoQuery(setInfoSpec));
            return new FedoraSetInfoIterator(m_fedora, tuples);
        }
    }

    /**
     * Convert the given Date to a String while also adding or subtracting a
     * millisecond. The shift is necessary because the provided dates are
     * inclusive, whereas ITQL date operators are exclusive.
     */
    protected String getExclusiveDateString(Date date, boolean isUntilDate) {
        if (date == null) {
            return null;
        } else {
            long time = date.getTime();
            if (isUntilDate) {
                time++; // add 1ms to make "until" -> "before"
            } else {
                time--; // sub 1ms to make "from" -> "after"
            }
            return DateUtility.convertDateToString(new Date(time));
        }
    }

    public RemoteIterator<FedoraRecord> listRecords(Date from,
                                                    Date until,
                                                    FedoraMetadataFormat format) {

        // Construct and get results of one to three queries, depending on conf

        // Parse and convert the dates once; they may be used more than once
        String afterUTC = getExclusiveDateString(from, false);
        String beforeUTC = getExclusiveDateString(until, true);

        // do primary query
        String primaryQuery =
                getListRecordsPrimaryQuery(afterUTC, beforeUTC, format
                        .getMetadataSpec());
        File primaryFile = getCSVResults(primaryQuery);

        // do set membership query, if applicable
        File setFile = null;
        if (m_itemSetSpecPath != null && m_itemSetSpecPath.length() > 0) { // need
            // set
            // membership
            // info
            String setQuery =
                    getListRecordsSetMembershipQuery(afterUTC,
                                                     beforeUTC,
                                                     format.getMetadataSpec());
            setFile = getCSVResults(setQuery);
        }

        // do about query, if applicable
        File aboutFile = null;
        if (format.getAboutSpec() != null) { // need
            // about
            // info
            String aboutQuery =
                    getListRecordsAboutQuery(afterUTC, beforeUTC, format);
            aboutFile = getCSVResults(aboutQuery);
        }

        // Get a FedoraRecordIterator over the combined results
        // that automatically cleans up the result files when closed

        String mdDissType = format.getMetadataSpec().getDisseminationType();
        String aboutDissType = null;

        if (format.getAboutSpec() != null) {
            aboutDissType = format.getAboutSpec().getDisseminationType();
        }

        try {
            ResultCombiner combiner =
                    new ResultCombiner(primaryFile, setFile, aboutFile, true);
            return new CombinerRecordIterator(format.getPrefix(),
                                              mdDissType,
                                              aboutDissType,
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
            return "$item     <" + MODEL.STATE + "> $state";
        } else {
            return "$item           <" + m_deleted + "> $state";
        }
    }

    private void appendDateParts(String afterUTC,
                                 String beforeUTC,
                                 boolean alwaysSelectDate,
                                 StringBuilder out) {
        if (afterUTC == null && beforeUTC == null && !alwaysSelectDate) {
            // we don't have to select the date because
            // there are no date constraints and the query doesn't ask for it
            return;
        } else {
            out.append("and    $item     <" + VIEW.LAST_MODIFIED_DATE
                    + "> $date\n");
        }

        // date constraints are optional
        if (afterUTC != null) {
            out.append("and    $date           <" + MULGARA.AFTER + "> '"
                    + afterUTC + "'^^<" + RDF_XSD.DATE_TIME + "> in <#xsd>\n");
        }
        if (beforeUTC != null) {
            out.append("and    $date           <" + MULGARA.BEFORE + "> '"
                    + beforeUTC + "'^^<" + RDF_XSD.DATE_TIME + "> in <#xsd>\n");
        }
    }

    // ordering is required for the combiner to work
    private void appendOrder(StringBuilder out) {
        out.append("order  by $itemID asc");
    }

    // this is common for all listRecords queries
    private void appendCommonFromWhereAnd(StringBuilder out) {
        out.append("from   <#ri>\n");
        out.append("where  $item           <" + m_oaiItemID + "> $itemID\n");
    }

    protected String getListRecordsPrimaryQuery(String afterUTC,
                                                String beforeUTC,
                                                InvocationSpec mdSpec) {
        StringBuilder out = new StringBuilder();

        String selectString = "";
        String contentDissString = "";

        selectString = "select $item $itemID $date $state\n";

        if (mdSpec.isDatastreamInvocation()) {
            contentDissString = getDatastreamDissType(mdSpec, "$item", "");
        } else {
            contentDissString = getServiceDissType(mdSpec, "$item", "");
        }

        out.append(selectString);
        appendCommonFromWhereAnd(out);
        out.append("and    " + getStatePattern() + "\n");

        out.append("and " + contentDissString);

        appendDateParts(afterUTC, beforeUTC, true, out);
        appendOrder(out);

        return out.toString();
    }

    protected String getListRecordsSetMembershipQuery(String afterUTC,
                                                      String beforeUTC,
                                                      InvocationSpec mdSpec) {
        StringBuilder out = new StringBuilder();

        out.append("select $itemID $setSpec\n");
        appendCommonFromWhereAnd(out);

        if (mdSpec.isDatastreamInvocation()) {
            out.append("and " + getDatastreamDissType(mdSpec, "$item", ""));
        } else {
            out.append("and " + getServiceDissType(mdSpec, "$item", ""));
        }
        appendDateParts(afterUTC, beforeUTC, true, out);
        out.append("and    " + m_itemSetSpecPath + "\n");
        appendOrder(out);

        return out.toString();
    }

    protected String getListRecordsAboutQuery(String afterUTC,
                                              String beforeUTC,
                                              FedoraMetadataFormat format) {
        StringBuilder out = new StringBuilder();

        InvocationSpec mdSpec = format.getMetadataSpec();
        InvocationSpec aboutSpec = format.getAboutSpec();

        out.append("select $itemID\n");

        appendCommonFromWhereAnd(out);
        if (mdSpec.isDatastreamInvocation()) {
            out.append("and " + getDatastreamDissType(mdSpec, "$item", "_md"));
        } else {
            out.append("and " + getServiceDissType(mdSpec, "$item", "_md"));
        }
        appendDateParts(afterUTC, beforeUTC, true, out);
        if (aboutSpec.isDatastreamInvocation()) {
            out.append("and "
                    + getDatastreamDissType(aboutSpec, "$item", "_about"));
        } else {
            out.append("and "
                    + getServiceDissType(aboutSpec, "$item", "_about"));
        }
        appendOrder(out);

        return out.toString();
    }

    /**
     * Get the results of the given itql tuple query as a temporary CSV file.
     */
    private File getCSVResults(String queryText) throws RepositoryException {

        logger.debug("getCSVResults() called with query:\n" + queryText);

        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", queryText);

        File tempFile = null;
        OutputStream out = null;
        try {
            tempFile =
                    File.createTempFile("oaiprovider-listrec-tuples", ".csv");
            tempFile.deleteOnExit(); // just in case
            out = new FileOutputStream(tempFile);
        } catch (IOException e) {
            throw new RepositoryException("Error creating temp query result file",
                                          e);
        }

        try {
            TupleIterator tuples = m_queryClient.getTuples(parameters);
            logger.debug("Saving query results to disk...");
            tuples.toStream(out, RDFFormat.CSV);
            logger.debug("Done saving query results");
            return tempFile;
        } catch (Exception e) {
            tempFile.delete();
            throw new RepositoryException("Error getting tuples from Fedora: "
                    + e.getMessage(), e);
        } finally {
            try {
                out.close();
            } catch (Exception e) {
            }
        }
    }

    private String getServiceDissType(InvocationSpec spec,
                                      String objectVar,
                                      String suffix) {

        StringBuilder s = new StringBuilder();
        String model = "$model" + suffix;
        String sDef = "$SDef" + suffix;
        s.append(objectVar + " <" + MODEL.HAS_MODEL + "> " + model
                + "\n");
        s.append("and " + model + " <" + MODEL.HAS_SERVICE + "> " + sDef + "\n");
        s.append("and " + sDef + " <" + MODEL.DEFINES_METHOD + "> '"
                + spec.method() + "'\n");
        if (spec.service() != null) {
            s.append(" and " + sDef + " <" + MULGARA.IS + "> <"
                    + spec.service().toURI() + ">\n");
        }
        return s.toString();
    }

    private String getDatastreamDissType(InvocationSpec spec,
                                         String objectVar,
                                         String suffix) {
        StringBuilder s = new StringBuilder();
        String dissemination = "$diss" + suffix;
        s.append(objectVar + " <" + VIEW.DISSEMINATES + "> " + dissemination
                + "\n");
        s.append("and " + dissemination + " <" + VIEW.DISSEMINATION_TYPE
                + "> <" + spec.getDisseminationType() + ">\n");
        return s.toString();
    }

    protected String getListSetInfoQuery(InvocationSpec setInfoSpec) {
        StringBuffer query = new StringBuffer();

        String setInfoDissQuery =
                "      $setDiss <test:noMatch> <test:noMatch>\n";
        String target = "$setDiss";
        String dissType = "";
        String commonWhereClause =
                "where $set <" + m_setSpec + "> $setSpec\n" + "and $set <"
                        + m_setSpecName + "> $setName\n";

        if (setInfoSpec != null) {

            dissType = setInfoSpec.getDisseminationType();
            if (setInfoSpec.isDatastreamInvocation()) {
                setInfoDissQuery =
                        getDatastreamDissType(setInfoSpec, "$set", "");
                target = "$diss";
            } else {
                setInfoDissQuery = getServiceDissType(setInfoSpec, "$set", "");
                target = "$SDef";
            }
        }

        query.append("select $set $setSpec $setName '" + dissType + "'\n"
                + "  subquery(" + "    select " + target + "\n	  from <#ri>\n");
        query.append(commonWhereClause);
        query.append("and " + setInfoDissQuery);
        query.append(")\n");
        query.append("from <#ri>" + commonWhereClause);
        return query.toString();
    }

    private TupleIterator getTuples(String query) throws RepositoryException {
        logger.debug("getTuples() called with query:\n" + query);
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", query);
        parameters.put("stream", "true"); // stream immediately from server

        try {
            return m_queryClient.getTuples(parameters);
        } catch (IOException e) {
            throw new RepositoryException("Error getting tuples from Fedora: "
                    + e.getMessage(), e);
        }
    }

    /**
     * @param itemSetSpecPath
     * @return the setSpec, in the form "$item <$predicate> $setSpec"
     * @throws RepositoryException
     */
    protected String parseItemSetSpecPath(String itemSetSpecPath)
            throws RepositoryException {
        String msg = "Required property, itemSetSpecPath, ";
        String[] path = itemSetSpecPath.split("\\s+");
        if (itemSetSpecPath.indexOf("$item") == -1) {
            throw new RepositoryException(msg + "must include \"$item\"");
        }
        if (itemSetSpecPath.indexOf("$setSpec") == -1) {
            throw new RepositoryException(msg + "must include \"$setSpec\"");
        }
        if (!itemSetSpecPath.matches("(\\$\\w+\\s+<\\S+>\\s+\\$\\w+\\s*)+")) {
            throw new RepositoryException(msg
                    + "must be of the form $item <predicate> $setSpec");
        }
        if (path.length == 3 && path[1].equals(m_setSpec)) {
            throw new RepositoryException(msg
                    + "may not use the same predicate as defined in setSpec");
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
            if (path[i].startsWith("$")
                    && !(path[i].equals("$item") || path[i].equals("$set") || path[i]
                            .equals("$setSpec"))) {
                sb.append(path[i].hashCode());
            }
        }
        return (sb.toString());
    }

}
