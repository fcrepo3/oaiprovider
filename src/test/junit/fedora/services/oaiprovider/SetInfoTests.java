
package fedora.services.oaiprovider;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.BeforeClass;
import org.junit.Test;

import proai.SetInfo;
import proai.driver.RemoteIterator;

import fedora.services.oaiprovider.test.DisseminationOrigin;
import fedora.services.oaiprovider.test.MetadataFormatSpec;

import static fedora.services.oaiprovider.test.DisseminationOrigin.*;
import static org.junit.Assert.*;

/**
 * Tests the setInfo capabilities of the driver
 * 
 * @author birkland
 */
public class SetInfoTests
        extends FedoraOAIDriverTest {

    private static final MetadataFormatSpec defaultMetadataSpec =
            new MetadataFormatSpec("oai_dc", DATASTREAM, NONE);

    private static final String TEST_SET_OBJECT_PATH =
            FOXML_TEST_DATA_DIR + "setObjects";

    @BeforeClass
    public static void loadSetObjects() {
        getClassSession().ingestFrom(new File(TEST_SET_OBJECT_PATH));
    }

    /** Make sure we can find sets in the first place */
    @Test
    public void listSetsTest() {
        examineSetInfo(NONE, "", 3);
    }

    /**
     * Should find set info for exactly one setin test data - one whose setInfo
     * is provided by a datastream. The correct one should contain the string
     * "Description for test set ss.d".
     */
    @Test
    public void datastreamInfoListSetsTest() throws Exception {
        /*
         * Make sure we grab the setInfo streams that are produced by the
         * desired datastream
         */
        examineSetInfo(DATASTREAM, "Description for test set ss.d", 1);

        /* Make sure we still get all sets */
        examineSetInfo(SERVICE, "", 3);
    }

    /**
     * Should find set info for exactly one set. in test data - one whose
     * setInfo is provided by a service invocation. The correct one should
     * contain the string "Description for test set ss.s".
     */
    @Test
    public void serviceInfoListSetsTest() throws Exception {
        /*
         * Make sure we grab the setInfo streams that are produced by the
         * desired service
         */
        examineSetInfo(SERVICE, "Description for test set ss.s", 1);

        /* Make sure we still get all sets */
        examineSetInfo(SERVICE, "", 3);
    }

    public void examineSetInfo(DisseminationOrigin setInfoOrigin,
                               String stringToFindInSetInfo,
                               int expected) {
        FedoraOAIDriver driver = getDriver(setInfoOrigin, defaultMetadataSpec);

        RemoteIterator<SetInfo> sets = driver.listSetInfo();
        int setCount = 0;
        try {
            while (sets.hasNext()) {
                SetInfo set = sets.next();
                StringWriter source = new StringWriter();
                set.write(new PrintWriter(source, true));
                if (source.toString().contains(stringToFindInSetInfo)) {
                    setCount++;
                }
            }
        } finally {
            sets.close();
        }

        assertEquals("Did not find desired quantity of setInfo data",
                     expected,
                     setCount);
    }
}
