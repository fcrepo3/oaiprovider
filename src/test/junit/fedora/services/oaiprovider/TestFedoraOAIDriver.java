package fedora.services.oaiprovider;

import java.io.*;
import java.util.*;
import java.text.*;

import junit.framework.*;

import proai.*;
import proai.driver.*;

import fedora.services.oaiprovider.*;

/**
 * @author Edwin Shin
 */
public class TestFedoraOAIDriver extends TestCase {

    private OAIDriver m_impl;

	public TestFedoraOAIDriver(String name) { super (name); }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestFedoraOAIDriver.class);
    }

    public void setUp() {
        m_impl = new FedoraOAIDriver();
        m_impl.init(System.getProperties());
    }

    //////////////////////////////////////////////////////////////////////////

    public void atestLatestDate() throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String latestDate = df.format(m_impl.getLatestDate());
        System.out.println("Latest Date was " + latestDate);
    }

    public void testIdentity() throws Exception {
        StringWriter writer = new StringWriter();
        m_impl.write(new PrintWriter(writer, true));
        System.out.println("Result of writeIdentity:\n" + writer.toString());
    }

    public void atestFormats() throws Exception {
        Iterator iter = m_impl.listMetadataFormats();
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

    public void atestSets() throws Exception {
        Iterator iter = m_impl.listSetInfo();
        while (iter.hasNext()) {
            SetInfo info = (SetInfo) iter.next();
            String spec = info.getSetSpec();
            System.out.println("Set spec = " + spec);
            StringWriter writer = new StringWriter();
            info.write(new PrintWriter(writer, true));
            System.out.println("Result of write: \n" + writer.toString());
        }
    }

    public void atestRecords() throws Exception {
        Iterator iter;
        Date fromDate = null;
        Date untilDate = m_impl.getLatestDate();

    }

    public void tearDown() {
    }
}
