package fedora.services.oaiprovider;

import java.util.Date;
import java.util.Properties;

import junit.framework.TestCase;

import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 * @author cwilper@cs.cornell.edu
 */
public class TestITQLQueryFactory extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestITQLQueryFactory.class);
    }

    // this test factory is used by multiple tests
    private ITQLQueryFactory getTestQueryFactory() throws Exception {
        Properties props = new Properties();
        props.put(FedoraOAIDriver.PROP_ITEMID, "http://www.openarchives.org/OAI/2.0/itemID");
        props.put(FedoraOAIDriver.PROP_SETSPEC, "http://www.openarchives.org/OAI/2.0/setSpec");
        props.put(FedoraOAIDriver.PROP_SETSPEC_NAME, "http://www.openarchives.org/OAI/2.0/setName");
        props.put(FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH, "$item <fedora-rels-ext:isMemberOf> $set $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec");
        
        ITQLQueryFactory factory = new ITQLQueryFactory();
        factory.init(null, null, props);
        return factory;
    }

    public void testListRecordsPrimaryQuery() throws Exception {

        ITQLQueryFactory factory = getTestQueryFactory();

        Date from = new Date(0L); // the epoch (1970-01-01T00:00:00 GMT)
        Date until = new Date(1000000000000L); // the billenium (2001-09-09T:01:46:40 UTC)

        String afterUTC  = factory.getExclusiveDateString(from, false);
        String beforeUTC = factory.getExclusiveDateString(until, true);

        String dissTypeURI = "info:fedora/*/oai_dc";

        String query = factory.getListRecordsPrimaryQuery(afterUTC,
                                                          beforeUTC,
                                                          dissTypeURI);

        String q = "select $item $itemID $date $state\n"
                 + "from   <#ri>\n"
                 + "where  $item           <http://www.openarchives.org/OAI/2.0/itemID> $itemID\n"
                 + "and    $item           <info:fedora/fedora-system:def/view#disseminates> $recordDiss\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/model#state> $state\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/*/oai_dc>\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#lastModifiedDate> $date\n"
                 + "and    $date           <http://tucana.org/tucana#after> '1969-12-31T23:59:59.999Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "and    $date           <http://tucana.org/tucana#before> '2001-09-09T01:46:40.001Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "order  by $itemID asc";

        assertEquals(q, query);
    }

    public void testListRecordsSetMembershipQuery() throws Exception {

        ITQLQueryFactory factory = getTestQueryFactory();

        Date from = new Date(0L); // the epoch (1970-01-01T00:00:00 GMT)
        Date until = new Date(1000000000000L); // the billenium (2001-09-09T:01:46:40 UTC)

        String afterUTC  = factory.getExclusiveDateString(from, false);
        String beforeUTC = factory.getExclusiveDateString(until, true);

        String dissTypeURI = "info:fedora/*/oai_dc";

        String query = factory.getListRecordsSetMembershipQuery(afterUTC,
                                                                beforeUTC,
                                                                dissTypeURI);

        String q = "select $itemID $setSpec\n"
                 + "from   <#ri>\n"
                 + "where  $item           <http://www.openarchives.org/OAI/2.0/itemID> $itemID\n"
                 + "and    $item           <info:fedora/fedora-system:def/view#disseminates> $recordDiss\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/*/oai_dc>\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#lastModifiedDate> $date\n"
                 + "and    $date           <http://tucana.org/tucana#after> '1969-12-31T23:59:59.999Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "and    $date           <http://tucana.org/tucana#before> '2001-09-09T01:46:40.001Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "and    $item <fedora-rels-ext:isMemberOf> $set and $set <http://www.openarchives.org/OAI/2.0/setSpec> $setSpec\n"
                 + "order  by $itemID asc";

        assertEquals(q, query);
    }

    public void testListRecordsAboutQuery() throws Exception {

        ITQLQueryFactory factory = getTestQueryFactory();

        Date from = new Date(0L); // the epoch (1970-01-01T00:00:00 GMT)
        Date until = new Date(1000000000000L); // the billenium (2001-09-09T:01:46:40 UTC)

        String afterUTC  = factory.getExclusiveDateString(from, false);
        String beforeUTC = factory.getExclusiveDateString(until, true);

        String dissTypeURI = "info:fedora/*/oai_dc";
        String aboutDissTypeURI = "info:fedora/*/about_oai_dc";

        String query = factory.getListRecordsAboutQuery(afterUTC,
                                                        beforeUTC,
                                                        dissTypeURI,
                                                        aboutDissTypeURI);

        String q = "select $itemID\n"
                 + "from   <#ri>\n"
                 + "where  $item           <http://www.openarchives.org/OAI/2.0/itemID> $itemID\n"
                 + "and    $item           <info:fedora/fedora-system:def/view#disseminates> $recordDiss\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/*/oai_dc>\n"
                 + "and    $recordDiss     <info:fedora/fedora-system:def/view#lastModifiedDate> $date\n"
                 + "and    $date           <http://tucana.org/tucana#after> '1969-12-31T23:59:59.999Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "and    $date           <http://tucana.org/tucana#before> '2001-09-09T01:46:40.001Z'^^<http://www.w3.org/2001/XMLSchema#dateTime> in <#xsd>\n"
                 + "and    $item           <info:fedora/fedora-system:def/view#disseminates> $aboutDiss\n"
                 + "and    $aboutDiss      <info:fedora/fedora-system:def/view#disseminationType> <info:fedora/*/about_oai_dc>\n"
                 + "order  by $itemID asc";

        assertEquals(q, query);
    }

    public void testParseItemSetSpecPath() {
        Properties props = new Properties();
        props.put(FedoraOAIDriver.PROP_ITEMID, "urn:baz");
        String ssp = "$item <urn:foo> $set $set <urn:bar> $setSpec";
        props.put(FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH, ssp);
        ITQLQueryFactory iqf = new ITQLQueryFactory();
        QueryFactory qf = (QueryFactory) iqf;
        qf.init(null, null, props);

        assertEquals("$item <urn:foo> $set and $set <urn:bar> $setSpec", 
                     iqf.parseItemSetSpecPath(ssp));
        
        String[] badSetPaths = {"foo",
                                "$item <predicate:foo> $set",
                                "$foo <predicate:bar> $setSpec",
                                "$item $predicate $setSpec",
                                "$item <urn:foo> $set and $set <urn:bar> $setSpec",
                                "$item <urn:foo> $set $set <urn:bar>",
                                "$item urn:baz $setSpec"
        };
        
        for (int i = 0; i < badSetPaths.length; i++) {
            try {
                iqf.parseItemSetSpecPath(badSetPaths[i]);
                fail("Should have failed with a RepositoryException");
            } catch (RepositoryException e) {
                assertTrue(e.getMessage().indexOf("Required property, itemSetSpecPath, ") != -1);
            }
        }
        
    }
}
