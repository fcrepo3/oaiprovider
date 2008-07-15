
package fedora.services.oaiprovider;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import fedora.test.LiveSystemTests;

@RunWith(Suite.class)
@SuiteClasses( {DateCorrectnessTests.class, ListRecordsPermutationTests.class,
        SetMembershipTests.class, SetInfoTests.class})
public class AllLiveTests
        extends LiveSystemTests {
}
