package fedora.services.oaiprovider;

import java.io.*;

public class ResultCombiner {

    private BufferedReader m_r1;
    private BufferedReader m_r2;
    private BufferedReader m_r3;

    private String m_l2;
    private String m_l3;

    private static final String _START = "START";

    public ResultCombiner(BufferedReader r1,
                    BufferedReader r2,
                    BufferedReader r3) {
        m_r1 = r1;
        m_r2 = r2;
        m_r3 = r3;
        m_l2 = _START;
        m_l3 = _START;
    }

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

    private boolean hasAbout(String itemID) {
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

    private String setSpecs(String itemID) {
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

    public void close() {
        try { m_r1.close(); } catch (Throwable th) { }
        try { m_r2.close(); } catch (Throwable th) { }
        try { m_r3.close(); } catch (Throwable th) { }
    }

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