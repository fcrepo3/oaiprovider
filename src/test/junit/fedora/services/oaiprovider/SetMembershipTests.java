
package fedora.services.oaiprovider;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import proai.driver.RemoteIterator;

import fedora.common.PID;
import fedora.services.oaiprovider.test.MetadataFormatSpec;

import static fedora.services.oaiprovider.test.DisseminationOrigin.*;
import static junit.framework.Assert.*;

/**
 * Tests that deal with with driver correctly interpreting and reporting set
 * membership.
 * 
 * @author birkland
 */
public class SetMembershipTests
        extends FedoraOAIDriverTest {

    private static final Date DAWN_OF_TIME = new Date(0);

    private static final String TEST_SET_OBJECT_PATH =
            FOXML_TEST_DATA_DIR + "setObjects";

    private static final Set<PID> sets = new HashSet<PID>();

    @BeforeClass
    public static void loadSets() {
        Set<PID> ingested =
                getClassSession().ingestFrom(new File(TEST_SET_OBJECT_PATH));
        for (PID pid : ingested) {
            if (pid.toString().contains("object")) {
                sets.add(pid);
            }
        }
    }

    @Test
    public void addMemberTest() throws Exception {
        PID someObject = getSomeObject();
        PID setObject = sets.iterator().next();

        FedoraOAIDriver driver = getDriver();

        /* Make sure our object does not have any sets */
        FedoraRecord record = getCurrentRecord(driver);
        assertTrue("Did not expect the object to have sets yet!",
                   getSets(record.getSourceInfo()).size() == 0);
        addSet(setObject, someObject);

        /* Make sure our set has been found! */
        record = getCurrentRecord(driver);
        assertTrue("Did not detect that the test object was in a set",
                   getSets(record.getSourceInfo()).size() == 1);
    }

    @Test
    public void removeMemberTest() throws Exception {
        PID someObject = getSomeObject();
        PID setObject = sets.iterator().next();

        FedoraOAIDriver driver = getDriver();

        /* Make sure our object does not have any sets */
        FedoraRecord record = getCurrentRecord(driver);
        assertTrue("Did not expect the object to have sets yet!",
                   getSets(record.getSourceInfo()).size() == 0);

        addSet(setObject, someObject);

        /* Make sure our set has been found! */
        record = getCurrentRecord(driver);
        assertTrue("Did not detect that the test object was in a set",
                   getSets(record.getSourceInfo()).size() == 1);

        removeSet(setObject, someObject);
        record = getCurrentRecord(driver);
        assertTrue("Expected the object to have no sets!", getSets(record
                .getSourceInfo()).size() == 0);
    }

    @Test
    public void multipleMembershipTest() throws Exception {
        PID someObject = getSomeObject();

        FedoraOAIDriver driver = getDriver();

        /* Make sure our object does not have any sets */
        FedoraRecord record = getCurrentRecord(driver);
        assertTrue("Did not expect the object to have sets yet!",
                   getSets(record.getSourceInfo()).size() == 0);

        for (PID set : sets) {
            addSet(set, someObject);
        }

        /* Make sure our set has been found! */
        record = getCurrentRecord(driver);
        assertTrue("Did not detect that the test object was in a set",
                   getSets(record.getSourceInfo()).size() == sets.size());
    }

    private void removeSet(PID setObject, PID toWhom) throws Exception {
        getClient()
                .getAPIM()
                .purgeRelationship(toWhom.toString(),
                                   "info:fedora/fedora-system:def/relations-external#isMemberOf",
                                   setObject.toURI(),
                                   false,
                                   null);
    }

    private void addSet(PID setObject, PID toWhom) throws Exception {
        getClient()
                .getAPIM()
                .addRelationship(toWhom.toString(),
                                 "info:fedora/fedora-system:def/relations-external#isMemberOf",
                                 setObject.toURI(),
                                 false,
                                 null);
    }

    private Set<String> getSets(String sourceInfo) {
        Set<String> sets = new HashSet<String>();
        String[] info = sourceInfo.split(" ");

        for (int i = 4; i < info.length; i++) {
            sets.add(info[i]);
        }

        return sets;
    }

    private FedoraRecord getCurrentRecord(FedoraOAIDriver driver) {
        Date NOW = new Date();
        RemoteIterator<FedoraRecord> records =
                driver.listRecords(DAWN_OF_TIME, NOW, "oai_dc");

        FedoraRecord record = null;
        try {
            while (records.hasNext()) {
                record = records.next();
                if (records.hasNext()) {
                    FedoraRecord spurious = records.next();
                    throw new RuntimeException("Expected to find only one record... found: "
                            + record.getItemID()
                            + " and "
                            + spurious.getItemID());
                }
            }
        } finally {
            records.close();
        }

        return record;
    }

    private FedoraOAIDriver getDriver() {
        return getDriver(NONE, new MetadataFormatSpec("oai_dc",
                                                      DATASTREAM,
                                                      NONE));
    }

    private PID getSomeObject() {
        return ingestItems(DATASTREAM, NONE, 1).iterator().next();
    }
}
