
package fedora.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;

import fedora.common.Constants;
import fedora.test.TransientFedoraSession.Protocol;

/**
 * Establishes and manages fedora {@link SystemConfig configuration} and
 * {@link TransientFedoraSession sessions} for live system tests.
 * <p>
 * If use as the base class for test suites, has an {@link AfterClass} method
 * that will assure that all sessions created during the suite have been
 * destroyed.
 * </p>
 * 
 * @author birkland
 */
public abstract class LiveSystemTests {

    private static final String PARAM_FLUSH = "fedora.test.flushUpdates";

    private static final Protocol PROTOCOL = Protocol.HTTP;

    private static final SystemConfig m_fedoraConfig =
            SystemConfig.buildFromFedoraHome(Constants.FEDORA_HOME);

    /**
     * If true, all subsequent sessions will automatically flush the
     * resourceIndex buffer after every write operation, "guaranteeing" their
     * immediate visibility.
     */
    public static boolean FLUSH_UPDATES =
            Boolean.getBoolean(System.getProperty(PARAM_FLUSH, "false"));

    private static final List<TransientFedoraSession> m_fedoraSessions =
            new ArrayList<TransientFedoraSession>();

    public static TransientFedoraSession newSession() {
        TransientFedoraSession session =
                new TransientFedoraSession(m_fedoraConfig,
                                           PROTOCOL,
                                           FLUSH_UPDATES);
        m_fedoraSessions.add(session);
        return session;
    }

    public static SystemConfig getFedoraConfig() {
        return m_fedoraConfig;
    }

    /** Destroy any test objects created */
    @AfterClass
    public static void clearSessions() {
        for (TransientFedoraSession session : m_fedoraSessions) {
            session.clear();
        }
    }
}
