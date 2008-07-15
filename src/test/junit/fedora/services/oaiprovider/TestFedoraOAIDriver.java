
package fedora.services.oaiprovider;

import java.util.*;
import java.text.*;

import junit.framework.*;

import proai.*;
import proai.driver.*;

/**
 * @author Edwin Shin
 */
public class TestFedoraOAIDriver
        extends TestCase {

    private OAIDriver m_impl;

    public TestFedoraOAIDriver(String name) {
        super(name);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestFedoraOAIDriver.class);
    }

    public void setUp() {
        m_impl = new FedoraOAIDriver();
        m_impl.init(System.getProperties());
    }

    //////////////////////////////////////////////////////////////////////////

    public void testLatestDate() throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String latestDate = df.format(m_impl.getLatestDate());
        System.out.println("Latest Date was " + latestDate);
    }

    public void testFormats() throws Exception {
        Iterator<? extends MetadataFormat> iter = m_impl.listMetadataFormats();
        while (iter.hasNext()) {
            MetadataFormat format = (MetadataFormat) iter.next();
            String prefix = format.getPrefix();
            String uri = format.getNamespaceURI();
            String loc = format.getSchemaLocation();
            System.out.println("Format prefix = " + prefix);
            System.out.println("       uri    = " + uri);
            System.out.println("       loc    = " + loc);
        }
    }

    public void latestRecords() throws Exception {
    }

    public void testGetOptional() {
        Properties p = new Properties();
        String key = "foo";
        String value = "bar";
        p.put(key, value);

        assertEquals(value, FedoraOAIDriver.getOptional(p, key));
        assertEquals("", FedoraOAIDriver.getOptional(p, value));

    }

    public void tearDown() {
    }
}
