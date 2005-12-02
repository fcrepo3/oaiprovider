package fedora.services.oaiprovider;

import java.io.*;

import org.apache.log4j.Logger;

/**
 * Combines three RDF query results into a single iterator that can be
 * used to construct FedoraRecord objects.
 *
 * <h2>Input</h2>
 *
 * The input is provided to the constructor using Files or BufferedReaders
 * in CSV format.
 *
 * <pre>
 * File 1:
 * "item","itemID","date","state"
 * info:fedora/nsdl:10013,oai:nsdl.org:nsdl:10011:nsdl:10013,2005-09-20T12:49:14.77,info:fedora/fedora-system:def/model#Active
 * info:fedora/nsdl:10015,oai:nsdl.org:nsdl:10011:nsdl:10015,2005-09-20T12:49:15.391,info:fedora/fedora-system:def/model#Active
 * info:fedora/nsdl:2051858,oai:nsdl.org:nsdl:10059:nsdl:2051858,2005-09-20T12:50:01,info:fedora/fedora-system:def/model#Active
 * info:fedora/nsdl:2052376,oai:nsdl.org:nsdl:10059:nsdl:2052376,2005-09-20T12:50:02.23.123,info:fedora/fedora-system:def/model#Active
 * 
 * File 2:
 * "itemID","setSpec"
 * oai:nsdl.org:nsdl:10059:nsdl:2051858,5101
 * oai:nsdl.org:nsdl:10059:nsdl:2051858,set2
 * oai:nsdl.org:nsdl:10059:nsdl:2052376,5101
 * oai:nsdl.org:nsdl:10059:nsdl:2052420,5101
 *
 * File 3:
 * "itemID"
 * oai:nsdl.org:nsdl:10011:nsdl:10013
 * oai:nsdl.org:nsdl:10011:nsdl:10015
 * oai:nsdl.org:nsdl:10059:nsdl:2051858
 * oai:nsdl.org:nsdl:10059:nsdl:2052376
 * </pre>
 *
 * <h2>Output</h2>
 *
 * The output is given one line at a time in the format below.  The header
 * is not actually provided, but is shown here for clarity.  Each item has
 * exactly one line with at least five comma-separated values.  And additional
 * value is provided for each set the item is a member of.
 *
 * <pre>
 * "item","itemID","date","state","hasAbout"[,"setSpec1"[,"setSpec2"[,...]]]
 * info:fedora/nsdl:10013,oai:nsdl.org:nsdl:10011:nsdl:10013,2005-09-20T12:49:14.77,info:fedora/fedora-system:def/model#Active,true
 * info:fedora/nsdl:10015,oai:nsdl.org:nsdl:10011:nsdl:10015,2005-09-20T12:49:15.391,info:fedora/fedora-system:def/model#Active,true
 * info:fedora/nsdl:2051858,oai:nsdl.org:nsdl:10059:nsdl:2051858,2005-09-20T12:50:01,info:fedora/fedora-system:def/model#Active,true,5101,set2
 * info:fedora/nsdl:2052376,oai:nsdl.org:nsdl:10059:nsdl:2052376,2005-09-20T12:50:02.23.123,info:fedora/fedora-system:def/model#Active,true,5101
 * </pre>
 *
 * @author cwilper@cs.cornell.edu
 */
public class ResultCombiner {

    private static final Logger logger =
        Logger.getLogger(FedoraOAIDriver.class.getName());

    private File m_f1;
    private File m_f2;
    private File m_f3;

    private boolean m_deleteOnClose;

    private BufferedReader m_r1;
    private BufferedReader m_r2;
    private BufferedReader m_r3;

    private String m_l2;
    private String m_l3;

    private static final String _START = "START";

    /**
     * Initialize with BufferedReaders containing the input data.
     *
     * @param r1 The results of the first query.  Must be provided.
     * @param r2 The results of the second query.  If given as null, output will
     *           not contain set membership information.
     * @param r3 The results of the thrid query.  If given as null, output will
     *           always indicate that no "about" information exist for the item.
     */
    public ResultCombiner(BufferedReader r1,
                          BufferedReader r2,
                          BufferedReader r3) {
        m_r1 = r1;
        m_r2 = r2;
        m_r3 = r3;
        m_l2 = _START;
        m_l3 = _START;
    }

    /**
     * Initialize with Files containing the input data, optionally deleting
     * them when the ResultCombiner is closed.
     *
     * @param f1 The results of the first query.  Must be provided.
     * @param f2 The results of the second query.  If given as null, output will
     *           not contain set membership information.
     * @param f3 The results of the thrid query.  If given as null, output will
     *           always indicate that no "about" information exist for the item.
     * @param deleteOnClose Whether to delete all provided files when this
     *                      ResultCombiner is closed.
     */
    public ResultCombiner(File f1, 
                          File f2, 
                          File f3, 
                          boolean deleteOnClose) throws FileNotFoundException {
        m_f1 = f1;
        m_f2 = f2;
        m_f3 = f3;
        m_deleteOnClose = deleteOnClose;
        m_r1 = new BufferedReader(
                   new InputStreamReader(
                       new FileInputStream(m_f1)));
        if (m_f2 != null) m_r2 = new BufferedReader(
                                     new InputStreamReader(
                                         new FileInputStream(m_f2)));
        if (m_f3 != null) m_r3 = new BufferedReader(
                                     new InputStreamReader(
                                         new FileInputStream(m_f3)));
        m_l2 = _START;
        m_l3 = _START;
    }

    /**
     * Get the next line of output, or null if we've reached the end.
     */
    public String readLine() {
        String l1 = nextLine(m_r1);
        if (l1 == null) {
            close();
            return null;
        } else {
            StringBuffer line = new StringBuffer();
            line.append(l1 + ",");
            String itemID = l1.split(",")[1];
            line.append(hasAbout(itemID));
            line.append(setSpecs(itemID));
            return line.toString();
        }
    }

    // tell whether this item has an about dissemination
    private boolean hasAbout(String itemID) {
        if (m_r3 == null) return false;
        if (m_l3 == _START) {
            m_l3 = nextLine(m_r3);
        }
        if (m_l3 == null) {
            return false;
        } else {
            if (m_l3.equals(itemID)) {
                m_l3 = nextLine(m_r3);
                return true;
            } else {
                return false;
            }
        }
    }

    // get comma-separated setSpecs for this item, or the empty string if none
    private String setSpecs(String itemID) {
        if (m_r2 == null) return "";
        if (m_l2 == _START) {
            m_l2 = nextLine(m_r2);
        }
        if (m_l2 == null) {
            return "";
        } else {
            StringBuffer out = new StringBuffer();
            while (m_l2 != null && m_l2.split(",")[0].equals(itemID)) {
                out.append("," + m_l2.split(",")[1]);
                m_l2 = nextLine(m_r2);
            }
            return out.toString();
        }
    }

    /**
     * Get the next line, skipping any that start with " or are blank.
     */
    private static String nextLine(BufferedReader r) {
        try {
            String line = r.readLine();
            while (line != null && (line.startsWith("\"") || (line.trim().equals("")))) {
                line = r.readLine();
            }
            if (line == null) {
                return null;
            } else {
                return line.trim();
            }
        } catch (IOException e) {
            System.err.println("WARNING: " + e.getMessage());
            return null;
        }
    }

    /**
     * Close the input readers and delete the associated files if the
     * combiner was constructed with the option to do so.
     */
    public void close() {
        try { m_r1.close(); } catch (Throwable th) { }
        try { m_r2.close(); } catch (Throwable th) { }
        try { m_r3.close(); } catch (Throwable th) { }
        if (m_deleteOnClose) {
            m_f1.delete();
            if (m_f2 != null) m_f2.delete();
            if (m_f3 != null) m_f3.delete();
        }
    }

    /**
     * A simple command-line test of this class.  Give it three filenames.
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            ResultCombiner c = new ResultCombiner(new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0])))),
                                      new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[1])))),
                                      new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[2])))));
            String line = c.readLine();
            while (line != null) {
                System.out.println(line);
                line = c.readLine();
            }
        } else {
            System.err.println("ERROR: Three args expected (csv files to be combined)");
        }
    }

}