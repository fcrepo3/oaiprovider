package fedora.services.oaiprovider;

import java.io.FileInputStream;

import org.trippi.RDFFormat;
import org.trippi.TupleIterator;

import junit.framework.TestCase;

/**
 * @author Edwin Shin
 */
public class TestFedoraRecordIterator extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestFedoraRecordIterator.class);
    }

    public void testFoo() throws Exception {
        TupleIterator ti = TupleIterator.fromStream(new FileInputStream("src/test/junit/risearch.sparql"), RDFFormat.SPARQL);
        FedoraRecordIterator fri = new FedoraRecordIterator(null, ti);
        while (fri.hasNext()) {
            fri.next();
        }
        
    }
}
