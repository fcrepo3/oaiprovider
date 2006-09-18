package fedora.services.oaiprovider;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;
import org.nsdl.mptstore.core.BasicTableManager;
import org.nsdl.mptstore.core.DDLGenerator;
import org.nsdl.mptstore.core.TableManager;
import org.nsdl.mptstore.query.GraphPattern;
import org.nsdl.mptstore.query.GraphQuery;
import org.nsdl.mptstore.query.GraphQuerySQLProvider;
import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.TripleFilter;
import org.nsdl.mptstore.query.TriplePattern;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;
import fedora.client.FedoraClient;
import fedora.common.Constants;
import fedora.server.utilities.DateUtility;


/** Directly queries a relational MPT resource index via SQL
 * 
 * @author birkland
 *
 */
public class MPTQueryFactory implements QueryFactory, Constants {

    private static final Logger logger =
        Logger.getLogger(MPTQueryFactory.class.getName());
    
	private static final String PROP_DDL = FedoraOAIDriver.NS + "mpt.db.ddlGenerator";
	private static final String PROP_URL = FedoraOAIDriver.NS + "mpt.jdbc.url";
	private static final String PROP_USERNAME = FedoraOAIDriver.NS + "mpt.jdbc.user";
	private static final String PROP_PASSWD = FedoraOAIDriver.NS + "mpt.jdbc.password";
    private static final String PROP_PREDICATE_MAP = FedoraOAIDriver.NS + "mpt.db.map";
    private static final String PROP_MAP_PREFIX = FedoraOAIDriver.NS + "mpt.db.prefix";
    private static final String  PROP_DB_DRIVER = FedoraOAIDriver.NS + "mpt.db.driverClassName";
	
	private String itemID;
	private String setSpec;
	private String setName;
	private String setSpecPath;
	private String setSpecDissType;
	private String deletedState;
	
    private TableManager adaptor;
    
    private FedoraClient client;
    private DataSource dataSource;
    
    private  Properties proaiProperties;
	
	public void init(FedoraClient client, FedoraClient queryClient,
			Properties props) {
		
        this.proaiProperties = props;
        this.client = client;
		this.adaptor = getTableManager(props);
		
		this.itemID = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);
		this.setSpec = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC);
		
		
		if (!this.setSpec.equals("")) {
			this.setName = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_SETSPEC_NAME);
			this.setSpecPath = FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH);
			this.setSpecDissType = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_SETSPEC_DESC_DISSTYPE);
		} 
		
		this.deletedState = FedoraOAIDriver.getOptional(props, FedoraOAIDriver.PROP_DELETED);
	}

	public Date latestRecordDate(Iterator fedoraMetadataFormats) {
		String mods = 
			adaptor.getTableFor("<info:fedora/fedora-system:def/view#lastModifiedDate>");
		
        logger.debug("getting latest record date");
		String date;
        Connection c;
		try {
            c = dataSource.getConnection();
			PreparedStatement s = c.prepareStatement("SELECT max(o) FROM " + mods , 
					ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
			ResultSet r = s.executeQuery();
            r.next();
			date = r.getString(1);
            c.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
		/* 
		 * Just to be on the safe side, if it's obvious we can't parse the
		 * date, then return 'now'.
		 */
        
        logger.debug("given " + date);
		if (date == null || !date.matches("\".+\"\\^\\^http://www.w3.org/2001/XMLSchema#dateTime")) {
            logger.debug("does not match expected format.  returning current time");
			return new Date();
		} else {
			String[] dateParts = date.split("\"");
            logger.debug("Using date " + dateParts[1]);
			return DateUtility.convertStringToDate(dateParts[1]);
		}
	}

	public RemoteIterator listRecords(Date from, Date until, String mdPrefix,
			String mdPrefixDissType, String mdPrefixAboutDissType) {
		
        String recordDiss = FedoraOAIDriver.getRequired(proaiProperties, FedoraOAIDriver.NS + 
                "md.format." + mdPrefix + ".dissType"); 
        
        String aboutDiss = FedoraOAIDriver.getOptional(proaiProperties, FedoraOAIDriver.NS + 
                "md.format." + mdPrefix + ".about.dissType");
        
		GraphQuery query = new GraphQuery();
		
		/* First, build the core required part of the query */
		GraphPattern requiredPath = new GraphPattern();
		requiredPath.addTriplePattern(new TriplePattern("$item", "<" + this.itemID + ">",  "$itemID"));
		requiredPath.addTriplePattern(new TriplePattern("$item", "<info:fedora/fedora-system:def/view#disseminates>", "$recordDiss"));
		requiredPath.addTriplePattern(new TriplePattern("$recordDiss", "<info:fedora/fedora-system:def/view#disseminationType>", "<" + mdPrefixDissType + ">"));
		requiredPath.addTriplePattern(new TriplePattern("$recordDiss", "<info:fedora/fedora-system:def/view#lastModifiedDate>", "$date"));
		requiredPath.addTriplePattern( new TriplePattern("$recordDiss", "<info:fedora/fedora-system:def/model#state>", "$state"));
        
		requiredPath.addFilter(new TripleFilter("$date", ">", DateUtility.convertDateToString(from)));
		/* Note: We add a milisecond to the 'until', in order to assure that the comparison is inclusive */ 
		requiredPath.addFilter(new TripleFilter("$date", "<", DateUtility.convertDateToString(new Date(until.getTime() + 1))));
		
        query.addRequired(requiredPath);
		
		/* Next, the setSpec, if asked for) */
		if (setSpecPath != null && setSpecPath.length() > 0) {
			query.addOptional(parseSetSpecPath(setSpecPath));
		}
		
		/* Next, the about */
		GraphPattern aboutPath = new GraphPattern();
		aboutPath.addTriplePattern(new TriplePattern("$item", "<info:fedora/fedora-system:def/view#disseminates>", "$aboutDiss"));
		aboutPath.addTriplePattern(new TriplePattern("$aboutDiss", "<info:fedora/fedora-system:def/view#disseminationType>", "<" + mdPrefixAboutDissType + ">"));
		query.addOptional(aboutPath);
		
        String[] targets = {"$item", "$itemID", "$date", "$state", "$setSpec", "$recordDiss", "$aboutDiss"};
        GraphQuerySQLProvider builder = new GraphQuerySQLProvider(adaptor, query);
        builder.setTargets(Arrays.asList(targets));
        builder.orderBy("$itemID", false);
        
        try {
            logger.info("Build ListRecords query " + builder.getSQL());
        } catch (QueryException e) {
            logger.error("Error building ListRecords query", e);
        }
		return new MPTItemIterator(client, builder, dataSource, mdPrefix, deletedState, recordDiss, aboutDiss);
	}

	public RemoteIterator listSetInfo() {
        
        GraphQuery query = new GraphQuery();
       
        /* First, the required bits */
        GraphPattern requiredPath = new GraphPattern();
        requiredPath.addTriplePattern(new TriplePattern("$set", "<" + setSpec + ">", "$setSpec"));
        requiredPath.addTriplePattern(new TriplePattern("$set", "<" + setName + ">", "$setName"));
        query.addRequired(requiredPath);
        
        /* Now, the optional dissemination */
        GraphPattern disseminationPath = new GraphPattern();
        disseminationPath.addTriplePattern(new TriplePattern("$set", "<" + VIEW.DISSEMINATES + ">", "$setDiss"));
        disseminationPath.addTriplePattern(new TriplePattern("$setDiss", "<" + VIEW.DISSEMINATION_TYPE.uri + ">", "<" + setSpecDissType + ">"));
        query.addOptional(disseminationPath);
        
        String[] targets = {"$set", "$setSpec", "$setName", "$setDiss"};
        GraphQuerySQLProvider builder = new GraphQuerySQLProvider(adaptor, query);
        builder.setTargets(Arrays.asList(targets));
        
        try {
            logger.info("Build SetInfo query " + builder.getSQL());
        } catch (QueryException e) {
            logger.error("Error building SetInfo query", e);
        }
        return new MPTSetInfoIterator(client, builder, dataSource);
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
        
        dbParams.setProperty("url", FedoraOAIDriver.getRequired(props, PROP_URL));
        dbParams.setProperty("username", FedoraOAIDriver.getRequired(props, PROP_USERNAME));
        dbParams.setProperty("password", FedoraOAIDriver.getRequired(props, PROP_PASSWD));
        dbParams.setProperty("driverClassName", FedoraOAIDriver.getRequired(props, PROP_DB_DRIVER));
        try {
            logger.info("USING DRIVER " + FedoraOAIDriver.getRequired(props, PROP_DB_DRIVER));
           Class.forName(FedoraOAIDriver.getRequired(props, PROP_DB_DRIVER));
           source = (BasicDataSource) BasicDataSourceFactory.createDataSource(dbParams);
        } catch (Exception e) {
            throw new RuntimeException("Could not establish database connection", e);
        }
        this.dataSource = source;
        
        /* Finally, create the table manager */
        BasicTableManager manager;
        String mapTable = FedoraOAIDriver.getRequired(props, PROP_PREDICATE_MAP);
        String prefix = FedoraOAIDriver.getRequired(props, PROP_MAP_PREFIX);
        try {
            manager = new BasicTableManager(source, generator, mapTable, prefix);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize table mapper", e);
        }
		return manager;
	}
	
	private GraphPattern parseSetSpecPath (String itql) {
		
		GraphPattern setSpecPattern = new GraphPattern();
		
		/* 
		 * First, make sure the 'itql' arc has some semblance of 
		 * correctness.  Throw out obvious errors
		 */
		String msg = "Required property, itemSetSpecPath, ";
        String[] path = itql.split("\\s+");
        if (itql.indexOf("$item") == -1) {
            throw new RepositoryException(msg + "must include \"$item\"");
        }
        if (itql.indexOf("$setSpec") == -1) {
            throw new RepositoryException(msg + "must include \"$setSpec\"");
        }
        if ( !itql.matches("(\\$\\w+\\s+<\\S+>\\s+\\$\\w+\\s*)+") ) {
            throw new RepositoryException(msg + "must be of the form $item <predicate> $setSpec");
        }
        if (path.length == 3 && path[1].equals(setSpec)) {
            throw new RepositoryException(msg + "may not use the same predicate as defined in setSpec");
        }
        if ((path.length % 3) != 0) {
        	throw new RepositoryException(msg + "Malformed query.  Number of nodes must be multiple of three");
        }
        
        for (int i = 0; i < path.length; i+=3) {
            logger.info("setSpec pattern: " + path[i] + " " + path[i+1] + " " +path[i+2] );
        	setSpecPattern.addTriplePattern(new TriplePattern(path[i], path[i+1], path[i+2]));
        }
        
        return setSpecPattern;
	}
}
