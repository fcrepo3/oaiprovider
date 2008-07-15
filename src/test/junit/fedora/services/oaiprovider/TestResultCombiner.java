
package fedora.services.oaiprovider;

import java.io.*;

import junit.framework.TestCase;

/**
 * @author cwilper@cs.cornell.edu
 */
public class TestResultCombiner
        extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestResultCombiner.class);
    }

    public void test1() throws Exception {
        doTest(1, true, true);
        doTest(1, false, true);
        doTest(1, true, false);
        doTest(1, false, false);
    }

    private void doTest(int n, boolean includeSets, boolean includeAbouts)
            throws Exception {

        System.out.println("Running TestResultCombiner.doTest(" + n + ", "
                + includeSets + ", " + includeAbouts + ")");

        File f1 = new File("src/test/junit/combiner-test" + n + "-input1.csv");

        // don't use file 2 if this test doesn't include sets
        File f2 = null;
        if (includeSets) {
            f2 = new File("src/test/junit/combiner-test" + n + "-input2.csv");
        }

        // don't use file 2 if this test doesn't include abouts
        File f3 = null;
        if (includeAbouts) {
            f3 = new File("src/test/junit/combiner-test" + n + "-input3.csv");
        }

        // which file specifies the expected output for this test?
        String suffix = "";
        if (!includeSets) {
            if (!includeAbouts) {
                suffix = "4";
            } else {
                suffix = "2";
            }
        } else if (!includeAbouts) {
            suffix = "3";
        }

        String outFilename = "combiner-test" + n + "-output" + suffix + ".csv";

        File ex = new File("src/test/junit/" + outFilename);

        // start combining, comparing actual output to expected output
        ResultCombiner combiner = new ResultCombiner(f1, f2, f3, false);

        BufferedReader expectedReader =
                new BufferedReader(new InputStreamReader(new FileInputStream(ex)));

        String expectedLine = expectedReader.readLine();

        // compare all lines, making sure actual output is not too short
        // and each line matches in content
        int lineNum = 0;
        while (expectedLine != null) {
            lineNum++;
            String line = combiner.readLine();
            assertNotNull("Too few output lines.  Expected at least " + lineNum,
                          line);
            assertEquals("Unexpected content on line " + lineNum
                                 + ".  Expected the following:\n"
                                 + expectedLine
                                 + "\n...but got the following:\n" + line,
                         expectedLine,
                         line);
            expectedLine = expectedReader.readLine();
        }

        // exhausted expected output, now check if there's more actual output
        assertNull("Expected only " + lineNum
                + " output lines, but got at least " + "one more", combiner
                .readLine());

    }
}
