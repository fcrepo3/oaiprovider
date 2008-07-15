
package fedora.services.oaiprovider;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;

import proai.driver.RemoteIterator;

import fedora.services.oaiprovider.test.DisseminationOrigin;
import fedora.services.oaiprovider.test.MetadataFormatSpec;

import static fedora.services.oaiprovider.test.DisseminationOrigin.*;
import static junit.framework.Assert.*;

/**
 * Tests listRecord correctness for different permutations of metadata and about
 * origins.
 * 
 * @author birkland
 */
public class ListRecordsPermutationTests
        extends FedoraOAIDriverTest {

    private static final Date DAWN_OF_TIME = new Date(0);

    private static Date INGEST_TIME = new Date();

    private static final String PREFIX = "oai_dc";

    @BeforeClass
    public static void populate() {
        getClassSession().ingestFrom(new File(FOXML_TEST_DATA_DIR));
        INGEST_TIME = new Date();
    }

    @Test
    public void listRecordsPermutation_cd_an() {
        doListRecords(DATASTREAM, NONE, 4);
    }

    @Test
    public void listRecordsPermutation_cd_ad() {
        doListRecords(DATASTREAM, DATASTREAM, 4);
    }

    @Test
    public void listRecordsPermutation_cd_as() {
        doListRecords(DATASTREAM, SERVICE, 4);
    }

    @Test
    public void listRecordsPermutation_cs_an() {
        doListRecords(SERVICE, NONE, 3);
    }

    @Test
    public void listRecordsPermutation_cs_ad() {
        doListRecords(SERVICE, DATASTREAM, 3);
    }

    @Test
    public void listRecordsPermutation_cs_as() {
        doListRecords(SERVICE, SERVICE, 3);
    }

    private FedoraOAIDriver getDriverFor(DisseminationOrigin mdOrigin,
                                         DisseminationOrigin aboutOrigin) {
        return getDriver(NONE, new MetadataFormatSpec("oai_dc",
                                                      mdOrigin,
                                                      aboutOrigin));
    }

    private void doListRecords(DisseminationOrigin mdOrigin,
                               DisseminationOrigin aboutOrigin,
                               int expectedRecords) {
        FedoraOAIDriver driver = getDriverFor(mdOrigin, aboutOrigin);

        String desiredItemIDPart = "c.d";
        if (mdOrigin == SERVICE) {
            desiredItemIDPart = "c.s";
        }

        RemoteIterator<FedoraRecord> records =
                driver.listRecords(DAWN_OF_TIME, INGEST_TIME, PREFIX);

        int recordCount = 0;
        try {
            while (records.hasNext()) {
                FedoraRecord record = records.next();
                assertTrue("Record does not have the right type of content dissemination ",
                           record.getItemID().contains(desiredItemIDPart));

                StringWriter source = new StringWriter();
                driver.writeRecordXML(record.getItemID(),
                                      record.getPrefix(),
                                      record.getSourceInfo(),
                                      new PrintWriter(source));
                assertTrue(source.toString().contains(record.getItemID()));

                /*
                 * If the record has abouts that match what the driver is
                 * configured to read, make sure the abouts of matching objects
                 * are written. Otherwise, make sure that none are!
                 */
                if (getAboutOrigin(record.getItemID()).equals(aboutOrigin)
                        && aboutOrigin != NONE) {
                    assertTrue("Failed to write abouts for "
                            + record.getItemID(), source.toString()
                            .contains("<about>"));
                } else {
                    assertFalse("Should not have written abouts for "
                            + record.getItemID(), source.toString()
                            .contains("<about>"));
                }
                recordCount++;
            }
        } finally {
            records.close();
        }

        assertEquals("Did not find the expected number of records",
                     expectedRecords,
                     recordCount);
    }

    private DisseminationOrigin getAboutOrigin(String itemId) {
        if (itemId.contains("a.n")) {
            return NONE;
        } else if (itemId.contains("a.d")) {
            return DATASTREAM;
        } else if (itemId.contains("a.s")) {
            return SERVICE;
        } else {
            throw new RuntimeException("Cannot determine abouts origin from "
                    + itemId);
        }
    }
}
