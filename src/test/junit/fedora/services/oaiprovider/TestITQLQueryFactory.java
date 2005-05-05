package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * @author Edwin Shin
 */
public class TestITQLQueryFactory extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestITQLQueryFactory.class);
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
        
        Date df = new Date(0L);
        Date du = new Date(1114185883000L);
        Map map = iqf.getListRecordsQuery(df, du, "info:fedora/*/oai_dc", "info:fedora/*/about_oai_dc", true);
        System.out.println((String)map.get("query"));
    }

}
