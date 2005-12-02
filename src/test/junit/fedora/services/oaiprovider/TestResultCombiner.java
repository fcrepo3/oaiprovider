package fedora.services.oaiprovider;

import java.io.*;

import junit.framework.TestCase;

/**
 * @author cwilper@cs.cornell.edu
 */
public class TestResultCombiner extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestResultCombiner.class);
    }

    public void test1() throws Exception {

        doTest(1);
    }

    private void doTest(int n) throws Exception {

        File f1 = new File("src/test/junit/combiner-test" + n + "-input1.csv");
        File f2 = new File("src/test/junit/combiner-test" + n + "-input2.csv");
        File f3 = new File("src/test/junit/combiner-test" + n + "-input3.csv");
        File ex = new File("src/test/junit/combiner-test" + n + "-output.csv");

        ResultCombiner combiner = new ResultCombiner(f1, f2, f3, false);

        BufferedReader expectedReader = new BufferedReader(
                                            new InputStreamReader(
                                                new FileInputStream(ex)));

        String expectedLine = expectedReader.readLine();

        // compare all lines, making sure actual output is not too short
        // and each line matches in content
        int lineNum = 0;
        while (expectedLine != null) {
            lineNum++;
            String line = combiner.readLine();    
            assertNotNull("Too few output lines.  Expected at least " + lineNum,
                    line);
            assertEquals("Unexpected content on line " 
                    + lineNum + ".  Expected the following:\n" + expectedLine 
                    + "\n...but got the following:\n" + line,
                    expectedLine, line);
            expectedLine = expectedReader.readLine();
        }

        // exhausted expected output, now check if there's more actual output
        assertNull("Expected only " + lineNum + " output lines, but got at least "
                 + "one more", combiner.readLine());

    }
}
