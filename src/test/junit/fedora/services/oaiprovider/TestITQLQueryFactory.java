package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * @author Edwin Shin
 */
public class TestITQLQueryFactory extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestITQLQueryFactory.class);
    }
    
    public void testGetLatestRecordDateQuery() throws Exception {
        Properties props = new Properties();
        props.put(FedoraOAIDriver.PROP_ITEMID, "http://www.openarchives.org/OAI/2.0/itemID");
        
        ITQLQueryFactory iqf = new ITQLQueryFactory();
        QueryFactory qf = (QueryFactory) iqf;
        qf.init(null, props);
        String query = iqf.getLatestRecordDateQuery();
        
        String q = "select $date\n" +
                   "  subquery(\n" + 
                   "    select $volatile\n" +
                   "    from <#ri>\n" +
                   "    where $x <http://www.openarchives.org/OAI/2.0/itemID> $y\n" +
                   "      and $x <info:fedora/fedora-system:def/view#disseminates> $z\n" +
                   "      and $z <info:fedora/fedora-system:def/view#isVolatile> $volatile\n" +
                   "      and $volatile <http://tucana.org/tucana#is> 'true'\n" +
                   "  )\n" +
                   "from <#ri>\n" +
                   "where $object <http://www.openarchives.org/OAI/2.0/itemID> $oaiItemID\n" +
                   "  and $object <info:fedora/fedora-system:def/view#disseminates> $diss\n" +
                   "  and $diss <info:fedora/fedora-system:def/view#lastModifiedDate> $date\n" +
                   "  order by $date desc\n" +
                   "  limit 1\n";
        assertEquals(q, query);
    }
    
    public void testListRecordsQuery() throws Exception {
        Properties props = new Properties();
        props.put(FedoraOAIDriver.PROP_ITEMID, "http://www.openarchives.org/OAI/2.0/itemID");
        props.put(FedoraOAIDriver.PROP_SETSPEC, "http://www.openarchives.org/OAI/2.0/setSpec");
        props.put(FedoraOAIDriver.PROP_SETSPEC_NAME, "http://www.openarchives.org/OAI/2.0/setName");
        props.put(FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH, "$item <fedora-rels-ext:isMemberOf> $set $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec");
        
        ITQLQueryFactory iqf = new ITQLQueryFactory();
        QueryFactory qf = (QueryFactory)iqf;
        qf.init(null, props);
        
        Date df = new Date(0L); // the epoch (1970-01-01T00:00:00 GMT)
        Date du = new Date(1000000000000L); // the billenium (2001-09-09T:01:46:40 UTC)
        
        
        String query = iqf.getListRecordsQuery(df, du, "info:fedora/*/oai_dc", "info:fedora/*/about_oai_dc", true);
        System.out.println(query);
    }
}
