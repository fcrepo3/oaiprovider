package fedora.services.oaiprovider;

import java.io.*;
import java.math.*;
import java.util.*;

/**
 * Generates Fedora objects for testing the oai provider.
 *
 * They'll start with demo:Item6 and go up from there.
 *
 * They'll be placed under three levels of directories beneath outputDir.
 *
 * 00/
 *    00/
 *       00/
 *          06.xml
 * 01/
 *    50/
 *       02/
 *          01.xml
 */
public class GenerateTestObjects {

    private File m_outputDir;
    private int m_howMany;

    public GenerateTestObjects(File outputDir, int howMany) throws Exception {

        m_outputDir = outputDir;
        m_howMany = howMany;

        m_outputDir.mkdirs();

        for (int i = 0; i < howMany; i++) {
            generateObject(6 + i);
        }
    }

    private void generateObject(int objectNum) throws Exception {

        // figure out the full path and make sure the directory exists
        int n = objectNum;
        int millions = n / 1000000;
        n = n % 1000000;
        int tenThousands = n / 10000;
        n = n % 10000;
        int hundreds = n / 100;
        n = n % 100;
        File file = new File(m_outputDir,
                             getPadded(millions) + "/"
                           + getPadded(tenThousands) + "/"
                           + getPadded(hundreds) + "/"
                           + getPadded(n) + ".xml");
        file.getParentFile().mkdirs();

        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileOutputStream(file));
            writeObject(objectNum, out);
        } finally {
            if (out != null) try { out.close(); } catch (Exception e) { }
        }
    }

    private void writeObject(int objectNum, PrintWriter out) {

        out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        out.println("<foxml:digitalObject PID=\"demo:Item" + objectNum + "\"");
        out.println("  xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-0.xsd\">");
        out.println("  <foxml:objectProperties>");
        out.println("    <foxml:property NAME=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\" VALUE=\"FedoraObject\"/>");
        out.println("    <foxml:property NAME=\"info:fedora/fedora-system:def/model#state\" VALUE=\"Active\"/>");
        out.println("    <foxml:property NAME=\"info:fedora/fedora-system:def/model#label\" VALUE=\"Item " + objectNum + "\"/>");
        out.println("    <foxml:property NAME=\"info:fedora/fedora-system:def/model#createdDate\" VALUE=\"2004-04-04T00:00:00Z\"/>");
        out.println("    <foxml:property NAME=\"info:fedora/fedora-system:def/view#lastModifiedDate\" VALUE=\"2004-12-04T00:00:00Z\"/>");
        out.println("  </foxml:objectProperties>");
        out.println("  <foxml:datastream CONTROL_GROUP=\"X\" ID=\"RELS-EXT\" STATE=\"A\" VERSIONABLE=\"true\">");
        out.println("    <foxml:datastreamVersion ID=\"RELS-EXT.0\" CREATED=\"2004-04-04T00:00:00Z\" LABEL=\"Relationships\" MIMETYPE=\"text/xml\">");
        out.println("      <foxml:xmlContent>");
        out.println("        <rdf:RDF xmlns:oai=\"http://www.openarchives.org/OAI/2.0/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:rel=\"info:fedora/fedora-system:def/relations-external#\">");
        out.println("          <rdf:Description rdf:about=\"info:fedora/demo:Item" + objectNum + "\">");
        out.println("            <oai:itemID>oai:example.org:item" + objectNum + "</oai:itemID>");

        String evenOrOdd;
        if (objectNum % 2 == 0) {
            evenOrOdd = "Even";
        } else {
            evenOrOdd = "Odd";
        }

        out.println("            <rel:isMemberOf rdf:resource=\"info:fedora/demo:SetAboveTwo" + evenOrOdd + "\"/>");

        if (isPrime(objectNum)) {
            out.println("            <rel:isMemberOf rdf:resource=\"info:fedora/demo:SetPrime\"/>");
        }

        out.println("          </rdf:Description>");
        out.println("        </rdf:RDF>");
        out.println("      </foxml:xmlContent>");
        out.println("    </foxml:datastreamVersion>");
        out.println("  </foxml:datastream>");
        out.println("  <foxml:datastream CONTROL_GROUP=\"X\" ID=\"DC\" STATE=\"A\" VERSIONABLE=\"true\">");
        out.println("    <foxml:datastreamVersion ID=\"DC1.0\" CREATED=\"2004-04-04T00:00:00Z\" LABEL=\"Dublin Core Metadata\" MIMETYPE=\"text/xml\">");
        out.println("      <foxml:xmlContent>");
        out.println("        <oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\">");
        out.println("          <dc:title>Item " + objectNum + "</dc:title>");
        out.println("          <dc:identifier>demo:Item" + objectNum + "</dc:identifier>");
        out.println("        </oai_dc:dc>");
        out.println("      </foxml:xmlContent>");
        out.println("    </foxml:datastreamVersion>");
        out.println("  </foxml:datastream>");

        out.println("  <foxml:datastream CONTROL_GROUP=\"X\" ID=\"oai_dc\" STATE=\"A\" VERSIONABLE=\"true\">");
        out.println("    <foxml:datastreamVersion ID=\"oai_dc.0\" CREATED=\"2004-04-04T00:00:00Z\" LABEL=\"oai_dc\" MIMETYPE=\"text/xml\">");
        out.println("      <foxml:xmlContent>");
        out.println("        <oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">");
        out.println("          <dc:identifier>urn:example:resource" + objectNum + "</dc:identifier>");
        out.println("        </oai_dc:dc>");
        out.println("      </foxml:xmlContent>");
        out.println("    </foxml:datastreamVersion>");
        out.println("  </foxml:datastream>");
        out.println("</foxml:digitalObject>");

    }

    private static String getPadded(int num) {
        if (num < 10) {
            return "0" + num;
        } else {
            return "" + num;
        }
    }

    /**
     * Tell whether the given number is a prime.
     */
    private static boolean isPrime(long n) {
        return BigInteger.valueOf(n).isProbablePrime(20); // .5^20 =~ 1/1M
    }

    public static void main(String[] args) throws Exception {
        new GenerateTestObjects(new File(args[0]), Integer.parseInt(args[1]));
    }

}