
package fedora.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import fedora.client.FedoraClient;

/**
 * Base class for individual live tests.
 * <p>
 * Manages fedora {@link TransientFedoraSession sessions} for the class and
 * individual test context. Specifically, it creates two types of contexts:
 * class-wide and test-wide.
 * </p>
 * <p>
 * Class-scope sessions persist during the duration of all tests, can be
 * accessed through the static method {@link #getClassSession()} and are intended to
 * be populated in static {@link BeforeClass} methods. These sessions are
 * automatically destroyed/cleaned after all tests have run.
 * </p>
 * <p>
 * Test-scope sessions exist only for the duration of a single test. May be
 * accessed through {@link #getSession()} within a test. {@link #getClient()}
 * returns a fedora client that exists within the current test-scope session for
 * a given test. The session is automatically destroyed/cleaned after the test
 * has run.
 * </p>
 * 
 * @author birkland
 */
public abstract class LiveTest {

    /** Class-scope session for the currently executing test class */
    private static TransientFedoraSession m_classContext;

    /** Test-scope sesssion for the currently executing test */
    private TransientFedoraSession m_testSession;

    /** Get the class-scope persistent fedora session */
    protected static TransientFedoraSession getClassSession() {
        return m_classContext;
    }

    /** Get the current test-scope session */
    protected TransientFedoraSession getSession() {
        return m_testSession;
    }

    /** Get a fedora client operating within the current test scope */
    protected FedoraClient getClient() {
        return m_testSession.getClient();
    }

    @BeforeClass
    public static void createClassContext() {
        m_classContext = LiveSystemTests.newSession();
    }

    @Before
    public void createIndividualTestContext() {
        m_testSession = LiveSystemTests.newSession();
    }

    @After
    public void destroyIndividualTestContext() {
        m_testSession.clear();
    }

    @AfterClass
    public static void destroyClassContext() {
        m_classContext.clear();
    }
}
