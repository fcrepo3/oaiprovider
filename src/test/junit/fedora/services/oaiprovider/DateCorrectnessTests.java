
package fedora.services.oaiprovider;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Test;

import proai.driver.RemoteIterator;

import fedora.common.PID;
import fedora.services.oaiprovider.test.MetadataFormatSpec;
import fedora.test.LiveSystemTests;

import static fedora.services.oaiprovider.test.DisseminationOrigin.*;

public class DateCorrectnessTests
        extends FedoraOAIDriverTest {

    private static final MetadataFormatSpec defaultMetadataSpec =
            new MetadataFormatSpec("oai_dc", DATASTREAM, NONE);

    /** Tests that the getLatestDate() method works on an empty repository. */
    @Test
    public void getLatestDateTest() throws Exception {
        FedoraOAIDriver driver = getDriver(NONE, defaultMetadataSpec);

        PID randomObject = null;

        for (File member : new File(FOXML_TEST_DATA_DIR)
                .listFiles()) {
            if (member.isFile()) {
                try {
                    randomObject =
                            getSession().ingestFrom(member).iterator().next();
                    break;
                } catch (Exception e) {
                    continue;
                }
            }
        }

        Date initialDate = driver.getLatestDate();

        getClient().getAPIM().modifyObject(randomObject.toString(),
                                           "A",
                                           null,
                                           null,
                                           "Test is touching object");

        Date changedDate = driver.getLatestDate();

        Assert
                .assertTrue("lastModifiedDate did not change with a modified object.  "
                                    + "original: "
                                    + initialDate.getTime()
                                    + ", after: " + changedDate.getTime(),
                            initialDate.before(changedDate));
    }

    /** Assures that the date argument in listRecords() works */
    @Test
    public void listRecordsDateTest() throws Exception {
        FedoraOAIDriver driver = getDriver(NONE, defaultMetadataSpec);

        Set<PID> ingested = new HashSet<PID>();

        /* Ingest all items whose content is in a datastream */
        Date beforeIngest = new Date();

        ingested.addAll(ingestItems(DATASTREAM, null));

        Date afterIngest = new Date();

        int foundRecordCount = 0;
        RemoteIterator<FedoraRecord> records =
                driver.listRecords(beforeIngest, afterIngest, "oai_dc");

        /* Count the records we get back by querying the ingest period */
        try {
            while (records.hasNext()) {
                records.next();
                foundRecordCount++;
            }
        } finally {
            records.close();
        }

        /* Sanity check: we should find everything we ingested */
        if (ingested.size() != foundRecordCount) {

        }
        Assert.assertEquals("Could not list all ingest records", ingested
                .size(), foundRecordCount);

        PID randomObject = ingested.iterator().next();

        /* Now modify an object */
        getClient().getAPIM().modifyObject(randomObject.toString(),
                                           "A",
                                           null,
                                           null,
                                           "Test is touching object");

        Date afterModify = new Date();

        /* We should find only one object after ingest, but before modification */
        records = driver.listRecords(afterIngest, afterModify, "oai_dc");

        try {
            Assert.assertNotNull(records.next().getItemID());
            Assert.assertFalse(records.hasNext());
        } finally {
            records.close();
        }

        /* We should not find any objects after the modification time */
        records = driver.listRecords(afterModify, new Date(), "oai_dc");

        try {
            Assert.assertFalse(records.hasNext());
        } finally {
            records.close();
        }
    }

    @After
    public void cleanup() {
        LiveSystemTests.newSession().clear();
    }
}
