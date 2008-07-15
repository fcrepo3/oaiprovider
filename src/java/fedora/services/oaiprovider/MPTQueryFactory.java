
package fedora.services.oaiprovider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;
import org.nsdl.mptstore.core.BasicTableManager;
import org.nsdl.mptstore.core.DDLGenerator;
import org.nsdl.mptstore.core.TableManager;
import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.component.BasicNodeFilter;
import org.nsdl.mptstore.query.component.BasicNodePattern;
import org.nsdl.mptstore.query.component.BasicTriplePattern;
import org.nsdl.mptstore.query.component.GraphPattern;
import org.nsdl.mptstore.query.component.GraphQuery;
import org.nsdl.mptstore.query.component.NodeFilter;
import org.nsdl.mptstore.query.component.NodePattern;
import org.nsdl.mptstore.query.component.TriplePattern;
import org.nsdl.mptstore.query.provider.GraphQuerySQLProvider;
import org.nsdl.mptstore.rdf.Node;
import org.nsdl.mptstore.rdf.ObjectNode;
import org.nsdl.mptstore.rdf.PredicateNode;
import org.nsdl.mptstore.rdf.SubjectNode;
import org.nsdl.mptstore.util.NTriplesUtil;

import proai.MetadataFormat;
import proai.SetInfo;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.client.FedoraClient;
import fedora.common.Constants;
import fedora.server.utilities.DateUtility;

/**
 * Directly queries a relational MPT resource index via SQL
 * 
 * @author birkland
 */
public class MPTQueryFactory
        implements QueryFactory, Constants {

    private static final Logger logger =
            Logger.getLogger(MPTQueryFactory.class.getName());

    public static final String PROP_DDL =
            FedoraOAIDriver.NS + "mpt.db.ddlGenerator";

    public static final String PROP_URL = FedoraOAIDriver.NS + "mpt.jdbc.url";

    public static final String PROP_USERNAME =
            FedoraOAIDriver.NS + "mpt.jdbc.user";

    public static final String PROP_PASSWD =
            FedoraOAIDriver.NS + "mpt.jdbc.password";

    public static final String PROP_PREDICATE_MAP =
            FedoraOAIDriver.NS + "mpt.db.map";

    public static final String PROP_MAP_PREFIX =
            FedoraOAIDriver.NS + "mpt.db.prefix";

    public static final String PROP_DB_DRIVER =
            FedoraOAIDriver.NS + "mpt.db.driverClassName";

    public static final String PROP_BACKSLASH_ESCAPE =
            FedoraOAIDriver.NS + "mpt.db.backslashIsEscape";

    private String itemID;

    private String setSpec;

    private String setName;

    private String setSpecPath;

    private String deletedState;

    private boolean backslashIsEscape = true;

    private TableManager adaptor;

    private FedoraClient client;

    private DataSource dataSource;

    public void init(FedoraClient client,
                     FedoraClient queryClient,
                     Properties props) {

        this.client = client;
        this.adaptor = getTableManager(props);

        this.itemID =
                FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);
        this.setSpec =
                FedoraOAIDriver
                        .getRequired(props, FedoraOAIDriver.PROP_SETSPEC);

        if (!this.setSpec.equals("")) {
            this.setName =
                    FedoraOAIDriver
                            .getRequired(props,
                                         FedoraOAIDriver.PROP_SETSPEC_NAME);
            this.setSpecPath =
                    FedoraOAIDriver
                            .getRequired(props,
                                         FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH);
        }

        this.deletedState =
                FedoraOAIDriver
                        .getOptional(props, FedoraOAIDriver.PROP_DELETED);
    }

    public Date latestRecordDate(Iterator<? extends MetadataFormat> fedoraMetadataFormats) {

        String mods;
        try {
            mods =
                    adaptor
                            .getTableFor(NTriplesUtil
                                    .parsePredicate("<info:fedora/fedora-system:def/view#lastModifiedDate>"));
        } catch (ParseException e) {
            /* Should never get here :) */
            throw new RuntimeException("Could not parse predicate ", e);
        }
        logger.debug("getting latest record date");
        String date;
        Connection c;
        try {
            c = dataSource.getConnection();
            PreparedStatement s =
                    c.prepareStatement("SELECT max(o) FROM " + mods,
                                       ResultSet.FETCH_FORWARD,
                                       ResultSet.CONCUR_READ_ONLY);
            ResultSet r = s.executeQuery();
            r.next();
            date = r.getString(1);
            c.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        /*
         * Just to be on the safe side, if it's obvious we can't parse the date,
         * then return 'now'.
         */

        logger.debug("given " + date);
        if (date == null
                || !date
                        .matches("\".+\"\\^\\^<http://www.w3.org/2001/XMLSchema#dateTime>")) {
            logger
                    .debug("does not match expected format.  returning current time");
            return new Date();
        } else {
            String[] dateParts = date.split("\"");
            logger.debug("Using date " + dateParts[1]);
            return DateUtility.convertStringToDate(dateParts[1]);
        }
    }

    public RemoteIterator<FedoraRecord> listRecords(Date from,
                                                    Date until,
                                                    FedoraMetadataFormat format) {

        InvocationSpec mdSpec = format.getMetadataSpec();
        InvocationSpec aboutSpec = format.getAboutSpec();

        GraphQuery query = new GraphQuery();

        GraphPattern requiredPath = new GraphPattern();
        try {
            /* First, build the core required part of the query */
            requiredPath = new GraphPattern();
            requiredPath.addTriplePattern(getPattern("$item", "<" + this.itemID
                    + ">", "$itemID"));
            requiredPath.addTriplePattern(getPattern("$item", "<"
                    + Constants.VIEW.LAST_MODIFIED_DATE + ">", "$date"));

            addSpecPatterns(mdSpec, requiredPath, "$item", "_md");

            /*
             * If deletetedState isn't set, then use the default (i.e. per
             * dissemination), else use the user-defined property at a
             * per-object basis (i.e applies for *all* datastreams)
             */
            if (deletedState.equals("")) {
                logger
                        .debug("No explicit deleted state, so using state of dissemination...");
                requiredPath.addTriplePattern(getPattern("$item", "<"
                        + Constants.MODEL.STATE + ">", "$state"));
            } else {
                logger
                        .debug("Explicit deleted state, so using state of object as "
                                + deletedState + "...");
                requiredPath.addTriplePattern(getPattern("$item", "<"
                        + deletedState + ">", "$state"));
            }

            requiredPath.addFilter(getFilter("$date", ">", "\""
                    + DateUtility
                            .convertDateToString(new Date(from.getTime() - 1))
                    + "\""));
            /*
             * Note: We add a milisecond to the 'until', in order to assure that
             * the comparison is inclusive
             */
            requiredPath.addFilter(getFilter("$date", "<", "\""
                    + DateUtility
                            .convertDateToString(new Date(until.getTime() + 1))
                    + "\""));
        } catch (ParseException e) {
            throw new RepositoryException("Could not parse itemID", e);
        }
        query.addRequired(requiredPath);

        /* Next, the setSpec, if asked for) */
        if (setSpecPath != null && setSpecPath.length() > 0) {
            query.addOptional(parseSetSpecPath(setSpecPath));
        }

        /* Next, the about */
        String aboutDissTarget = null;
        GraphPattern aboutPath = new GraphPattern();
        try {
            aboutDissTarget = addSpecPatterns(aboutSpec, aboutPath, "$item", "_about");
        } catch (ParseException e) {
            throw new RepositoryException("Could not parse metadata about dissemination type \n",
                                          e);
        }
        query.addOptional(aboutPath);

        List<String> targets =
                new LinkedList<String>(Arrays.asList("$item",
                                                     "$itemID",
                                                     "$date",
                                                     "$state",
                                                     "$setSpec"));
        
        if (aboutDissTarget != null) {
            targets.add(aboutDissTarget);
        }

        GraphQuerySQLProvider builder =
                new GraphQuerySQLProvider(adaptor, query, backslashIsEscape);
        builder.setTargets(targets);
        builder.orderBy("$itemID", false);

        try {
            logger.debug("Using ListRecords query " + builder.getSQL());
        } catch (QueryException e) {
            logger.error("Error building ListRecords query", e);
        }
        return new MPTItemIterator(builder, dataSource, format, aboutDissTarget);
    }

    public RemoteIterator<SetInfo> listSetInfo(InvocationSpec setInfoSpec) {

        if (this.setSpecPath == null) {
            // return empty iterator if sets not configured
            return new FedoraSetInfoIterator();
        }

        GraphQuery query = new GraphQuery();

        /* First, the required bits */
        GraphPattern requiredPath = new GraphPattern();
        try {
            requiredPath.addTriplePattern(getPattern("$set", "<" + setSpec
                    + ">", "$setSpec"));
            requiredPath.addTriplePattern(getPattern("$set", "<" + setName
                    + ">", "$setName"));
        } catch (ParseException e) {
            throw new RepositoryException("could not parse setSpec or setName\n",
                                          e);
        }
        query.addRequired(requiredPath);

        /* Now, the optional dissemination */
        GraphPattern disseminationPath = new GraphPattern();

        String dissTarget = null;
        try {
            dissTarget =
                    addSpecPatterns(setInfoSpec,
                                    disseminationPath,
                                    "$set",
                                    "_spec");
        } catch (ParseException e) {
            throw new RepositoryException("Could not parse set info dissemination path\n",
                                          e);
        }
        query.addOptional(disseminationPath);

        List<String> targets = new ArrayList<String>();
        targets.addAll(Arrays.asList(new String[] {"$set", "$setSpec",
                "$setName"}));

        if (dissTarget != null) {
            targets.add(dissTarget);
        }

        GraphQuerySQLProvider builder =
                new GraphQuerySQLProvider(adaptor, query, backslashIsEscape);
        builder.setTargets(targets);

        try {
            logger.debug("Using SetInfo query " + builder.getSQL());
        } catch (QueryException e) {
            logger.error("Error building SetInfo query", e);
        }
        return new MPTSetInfoIterator(client,
                                      builder,
                                      dataSource,
                                      dissTarget,
                                      setInfoSpec);
    }

    private TableManager getTableManager(Properties props) {

        /* Initialize the DDL generator */
        DDLGenerator generator;
        try {
            String ddlGen = FedoraOAIDriver.getRequired(props, PROP_DDL);
            generator = (DDLGenerator) Class.forName(ddlGen).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize DDL generator", e);
        }

        /* Initialize database connection */
        BasicDataSource source;
        Properties dbParams = new Properties();

        dbParams.setProperty("url", FedoraOAIDriver
                .getRequired(props, PROP_URL));
        dbParams.setProperty("username", FedoraOAIDriver
                .getRequired(props, PROP_USERNAME));
        dbParams.setProperty("password", FedoraOAIDriver
                .getRequired(props, PROP_PASSWD));
        dbParams.setProperty("driverClassName", FedoraOAIDriver
                .getRequired(props, PROP_DB_DRIVER));
        try {
            logger.debug("USING DRIVER "
                    + FedoraOAIDriver.getRequired(props, PROP_DB_DRIVER));
            Class.forName(FedoraOAIDriver.getRequired(props, PROP_DB_DRIVER));
            source =
                    (BasicDataSource) BasicDataSourceFactory
                            .createDataSource(dbParams);
        } catch (Exception e) {
            throw new RuntimeException("Could not establish database connection",
                                       e);
        }
        this.dataSource = source;

        String escape =
                FedoraOAIDriver.getOptional(props, PROP_BACKSLASH_ESCAPE);
        if (escape != null) {
            backslashIsEscape = Boolean.getBoolean(escape);
        }

        /* Finally, create the table manager */
        BasicTableManager manager;
        String mapTable =
                FedoraOAIDriver.getRequired(props, PROP_PREDICATE_MAP);
        String prefix = FedoraOAIDriver.getRequired(props, PROP_MAP_PREFIX);
        try {
            manager =
                    new BasicTableManager(source, generator, mapTable, prefix);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize table mapper", e);
        }
        return manager;
    }

    private GraphPattern parseSetSpecPath(String itql) {

        GraphPattern setSpecPattern = new GraphPattern();

        /*
         * First, make sure the 'itql' arc has some semblance of correctness.
         * Throw out obvious errors
         */
        String msg = "Required property, itemSetSpecPath, ";
        String[] path = itql.split("\\s+");
        if (itql.indexOf("$item") == -1) {
            throw new RepositoryException(msg + "must include \"$item\"");
        }
        if (itql.indexOf("$setSpec") == -1) {
            throw new RepositoryException(msg + "must include \"$setSpec\"");
        }
        if (!itql.matches("(\\$\\w+\\s+<\\S+>\\s+\\$\\w+\\s*)+")) {
            throw new RepositoryException(msg
                    + "must be of the form $item <predicate> $setSpec");
        }
        if (path.length == 3 && path[1].equals(setSpec)) {
            throw new RepositoryException(msg
                    + "may not use the same predicate as defined in setSpec");
        }
        if ((path.length % 3) != 0) {
            throw new RepositoryException(msg
                    + "Malformed query.  Number of nodes must be multiple of three");
        }

        for (int i = 0; i < path.length; i += 3) {
            logger.debug("setSpec pattern: " + path[i] + " " + path[i + 1]
                    + " " + path[i + 2]);
            try {
                setSpecPattern
                        .addTriplePattern(getPattern(path[i],
                                                     translate(path[i + 1]),
                                                     path[i + 2]));
            } catch (ParseException e) {
                throw new RepositoryException("Could not parse setSpec pattern");
            }
        }

        return setSpecPattern;
    }

    private String translate(String input) {
        return input.replace("<fedora-rels-ext:", "<" + Constants.RELS_EXT.uri);
    }

    private static String addSpecPatterns(InvocationSpec spec,
                                          GraphPattern g,
                                          String target,
                                          String differentiator)
            throws ParseException {
        if (spec == null) {
            return null;
        } else if (spec.isDatastreamInvocation()) {
            addDatastreamDissPatterns(spec, g, target, differentiator);
            return "$diss" + differentiator;
        } else {
            addServiceDissPatterns(spec, g, target, differentiator);
            return "$SDef" + differentiator;
        }
    }

    private static void addDatastreamDissPatterns(InvocationSpec spec,
                                                  GraphPattern g,
                                                  String target,
                                                  String differentiator)
            throws ParseException {
        String dissemination = "$diss" + differentiator;
        String disseminationType = "<info:fedora/*/" + spec.method() + ">";
        g.addTriplePattern(getPattern(target, "<" + Constants.VIEW.DISSEMINATES
                + ">", dissemination));
        g.addTriplePattern(getPattern(dissemination, "<"
                + Constants.VIEW.DISSEMINATION_TYPE + ">", disseminationType));
    }

    private static void addServiceDissPatterns(InvocationSpec spec,
                                               GraphPattern g,
                                               String target,
                                               String differentiator)
            throws ParseException {
        String model_var = "$model" + differentiator;
        String sDef_var = "$SDef" + differentiator;
        String method_var = "$method" + differentiator;
        g.addTriplePattern(getPattern(target, "<"
                + Constants.MODEL.HAS_MODEL + ">", model_var));
        g.addTriplePattern(getPattern(model_var, "<" + Constants.MODEL.HAS_SERVICE
                + ">", sDef_var));
        g.addTriplePattern(getPattern(sDef_var, "<"
                + Constants.MODEL.DEFINES_METHOD + ">", method_var));

        g.addFilter(getFilter(method_var, "=", '"' + spec.method() + '"'));

        if (spec.service() != null) {
            g.addFilter(getFilter(sDef_var, "=", "<" + spec.service().toURI()
                    + ">"));
        }
    }

    private static TriplePattern getPattern(String s, String p, String o)
            throws ParseException {
        NodePattern<SubjectNode> subject;
        NodePattern<PredicateNode> predicate;
        NodePattern<ObjectNode> object;

        if (s.startsWith("$")) {
            subject = new BasicNodePattern<SubjectNode>(s);
        } else {
            subject =
                    new BasicNodePattern<SubjectNode>(NTriplesUtil
                            .parseSubject(s));
        }

        predicate =
                new BasicNodePattern<PredicateNode>(NTriplesUtil
                        .parsePredicate(p));

        if (o.startsWith("$")) {
            object = new BasicNodePattern<ObjectNode>(o);
        } else {
            object =
                    new BasicNodePattern<ObjectNode>(NTriplesUtil
                            .parseObject(o));
        }

        return new BasicTriplePattern(subject, predicate, object);
    }

    private static NodeFilter<Node> getFilter(String node,
                                              String operator,
                                              String value)
            throws ParseException {
        NodePattern<Node> n;
        NodePattern<Node> v;

        if (node.startsWith("$")) {
            n = new BasicNodePattern<Node>(node);
        } else {
            n = new BasicNodePattern<Node>(NTriplesUtil.parseObject(node));
        }

        if (value.startsWith("$")) {
            v = new BasicNodePattern<Node>(value);
        } else {
            v = new BasicNodePattern<Node>(NTriplesUtil.parseObject(value));
        }

        return new BasicNodeFilter<Node>(n, operator, v);
    }
}
