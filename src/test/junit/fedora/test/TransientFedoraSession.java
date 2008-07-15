
package fedora.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.MalformedURLException;

import java.rmi.RemoteException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import fedora.client.FedoraClient;

import fedora.common.Constants;
import fedora.common.PID;

import fedora.server.management.FedoraAPIM;
import fedora.server.types.gen.Datastream;
import fedora.server.types.gen.RelationshipTuple;

import static fedora.test.SystemConfig.PARAM_HOST;
import static fedora.test.SystemConfig.PARAM_PORT;
import static fedora.test.SystemConfig.PARAM_SSL_PORT;

/**
 * Used for facilitating non-durable tests in a Fedora repository.
 * 
 * @author Aaron Birkland
 */
public class TransientFedoraSession {

    private static final Logger LOG =
            Logger.getLogger(TransientFedoraSession.class);

    /*
     * XXX: Should probably be Set<PID>, but PID does not have .equals() or
     * .hashCode()
     */
    private final Set<String> m_ingested = new HashSet<String>();

    private static final String FEDORA_WEBAPP_BASE = "fedora";

    private final WrappedClient m_client;

    private final FedoraAPIM m_apim;

    private final String m_baseURL;

    private final boolean m_flush;

    public TransientFedoraSession(SystemConfig config,
                                  Protocol protocol,
                                  boolean flushTriples) {
        Map<String, String> params = config.getParameters();

        String port = params.get(PARAM_PORT);

        if (protocol == Protocol.HTTPS) {
            port = params.get(PARAM_SSL_PORT);
        }

        m_baseURL =
                protocol + "://" + params.get(PARAM_HOST) + ":" + port + "/"
                        + FEDORA_WEBAPP_BASE;

        m_flush = flushTriples;

        try {
            m_client =
                    new WrappedClient(m_baseURL, config.getUser(), config
                            .getPass());
            m_apim = m_client.getAPIM();
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not create client", e);
        }
    }

    /**
     * Ingest FOXML-1.1 files from a given directory or file.
     * <p>
     * Ingests from a single file or recursively (barring any hidden files or
     * folders) from a directory.
     * </p>
     * 
     * @param source
     *        File or directory containing foxml 1.1 files.
     * @return Set of al PIDS ingested.
     */
    public Set<PID> ingestFrom(File source) {
        HashSet<PID> ingested = new HashSet<PID>();

        LOG.debug("ingesting from " + source.getAbsolutePath());
        if (source.isHidden() || source.getName().startsWith(".")) {
            LOG.debug(source.getAbsolutePath() + " is hidden, skipping");
            return ingested;
        } else if (source.isDirectory()) {
            for (File file : source.listFiles()) {
                ingested.addAll(ingestFrom(file));
            }
        } else {
            try {
                String pid =
                        m_apim.ingest(getBytes(source),
                                      Constants.FOXML1_1.uri,
                                      "Ingested temporary object");
                ingested.add(PID.getInstance(pid));
            } catch (Exception e) {
                LOG.warn("Could not ingest from file "
                        + source.getAbsolutePath(), e);
            }
        }
        return ingested;
    }

    public FedoraClient getClient() {
        return m_client;
    }

    public String getBaseURL() {
        return m_baseURL;
    }

    public void flushRI() {
        InputStream s = null;
        try {
            s = m_client.get("/risearch?flush=true", true);
        } catch (Exception e) {
            throw new RuntimeException("Could not flush triples", e);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    LOG.warn(e);
                }
            }
        }
    }

    /**
     * "Undoes" any changes to the repository through this session.
     * <p>
     * Actually,this just purges any objects that were created through this
     * session. Changes to pre-existing objects are, for the present, not
     * addressed.
     * </p>
     */
    public void clear() {
        Set<String> toRemove = new HashSet<String>();
        synchronized (m_ingested) {
            for (String pid : new HashSet<String>(m_ingested)) {
                try {
                    m_apim.purgeObject(pid.toString(),
                                       "Removing temporary object",
                                       false);
                    LOG.info("Cleaning up object " + pid);
                    toRemove.add(pid);
                } catch (Exception e) {
                    LOG.warn("Could not clean up object " + pid, e);
                }
            }
        }
    }

    private byte[] getBytes(File src) {
        try {
            FileInputStream fs = new FileInputStream(src);
            ByteArrayOutputStream out =
                    new ByteArrayOutputStream((int) src.length());
            byte[] buffer = new byte[4096];
            int len;

            while ((len = fs.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }

            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Could not read file", e);
        }
    }

    /** Represents http or https protocols */
    public enum Protocol {
        HTTP("http"), HTTPS("https");

        private String m_name;

        private Protocol(String name) {
            m_name = name;
        }

        public String toString() {
            return m_name;
        }
    }

    private class WrappedClient
            extends FedoraClient {

        public WrappedClient(String baseURL, String user, String pass)
                throws MalformedURLException {
            super(baseURL, user, pass);
        }

        public FedoraAPIM getAPIM() {
            try {
                return new APIMWrapper(super.getAPIM());
            } catch (Exception e) {
                throw new RuntimeException("Could not open APIM", e);
            }
        }
    }

    private class APIMWrapper
            implements FedoraAPIM {

        private final FedoraAPIM m_delegate;

        public APIMWrapper(FedoraAPIM delegate) {
            if (delegate == null) {
                throw new NullPointerException("Delegate is null!");
            }
            m_delegate = delegate;
        }

        private void checkDurability(String pid) {
            if (!m_ingested.contains(PID.getInstance(pid).toString())) {
                LOG.warn("Modifying pre-existing object " + pid
                        + ", changes will not be undone upon clear().");
            }
        }

        private void flushIfNecessary() {
            if (m_flush) flushRI();
        }

        public String ingest(byte[] objectXML, String format, String logMessage)
                throws RemoteException {
            String pid = null;
            synchronized (m_ingested) {
                pid = m_delegate.ingest(objectXML, format, logMessage);
                m_ingested.add(PID.getInstance(pid).toString());
            }
            LOG.info("Ingested " + pid);
            flushIfNecessary();
            return pid;
        }

        public String modifyObject(String pid,
                                   String state,
                                   String label,
                                   String ownerId,
                                   String logMessage)
                throws java.rmi.RemoteException {

            checkDurability(pid);
            String retval =
                    m_delegate.modifyObject(pid,
                                            state,
                                            label,
                                            ownerId,
                                            logMessage);
            flushIfNecessary();
            return retval;
        }

        public byte[] getObjectXML(String pid) throws java.rmi.RemoteException {
            return m_delegate.getObjectXML(pid);
        }

        public byte[] export(String pid, String format, String context)
                throws java.rmi.RemoteException {
            return m_delegate.export(pid, format, context);
        }

        public java.lang.String purgeObject(java.lang.String pid,
                                            java.lang.String logMessage,
                                            boolean force)
                throws java.rmi.RemoteException {
            String timestamp = null;

            synchronized (m_ingested) {
                timestamp = m_delegate.purgeObject(pid, logMessage, force);
                String toRemove = PID.getInstance(pid).toString();
                if (!m_ingested.contains(toRemove)) {
                    LOG.warn("Purging pre-existing object " + pid
                            + ", it will not be restored upon clear()");
                }

                m_ingested.remove(toRemove);
            }
            flushIfNecessary();
            return timestamp;
        }

        public java.lang.String addDatastream(java.lang.String pid,
                                              java.lang.String dsID,
                                              java.lang.String[] altIDs,
                                              java.lang.String dsLabel,
                                              boolean versionable,
                                              java.lang.String MIMEType,
                                              java.lang.String formatURI,
                                              java.lang.String dsLocation,
                                              java.lang.String controlGroup,
                                              java.lang.String dsState,
                                              java.lang.String checksumType,
                                              java.lang.String checksum,
                                              java.lang.String logMessage)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String retval =
                    m_delegate.addDatastream(pid,
                                             dsID,
                                             altIDs,
                                             dsLabel,
                                             versionable,
                                             MIMEType,
                                             formatURI,
                                             dsLocation,
                                             controlGroup,
                                             dsState,
                                             checksumType,
                                             checksum,
                                             logMessage);
            flushIfNecessary();
            return retval;
        }

        public java.lang.String modifyDatastreamByReference(java.lang.String pid,
                                                            java.lang.String dsID,
                                                            java.lang.String[] altIDs,
                                                            java.lang.String dsLabel,
                                                            java.lang.String MIMEType,
                                                            java.lang.String formatURI,
                                                            java.lang.String dsLocation,
                                                            java.lang.String checksumType,
                                                            java.lang.String checksum,
                                                            java.lang.String logMessage,
                                                            boolean force)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String retval =
                    m_delegate.modifyDatastreamByReference(pid,
                                                           dsID,
                                                           altIDs,
                                                           dsLabel,
                                                           MIMEType,
                                                           formatURI,
                                                           dsLocation,
                                                           checksumType,
                                                           checksum,
                                                           logMessage,
                                                           force);
            flushIfNecessary();
            return retval;
        }

        public java.lang.String modifyDatastreamByValue(java.lang.String pid,
                                                        java.lang.String dsID,
                                                        java.lang.String[] altIDs,
                                                        java.lang.String dsLabel,
                                                        java.lang.String MIMEType,
                                                        java.lang.String formatURI,
                                                        byte[] dsContent,
                                                        java.lang.String checksumType,
                                                        java.lang.String checksum,
                                                        java.lang.String logMessage,
                                                        boolean force)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String retval =
                    m_delegate.modifyDatastreamByValue(pid,
                                                       dsID,
                                                       altIDs,
                                                       dsLabel,
                                                       MIMEType,
                                                       formatURI,
                                                       dsContent,
                                                       checksumType,
                                                       checksum,
                                                       logMessage,
                                                       force);
            flushIfNecessary();
            return retval;
        }

        public java.lang.String setDatastreamState(String pid,
                                                   String dsID,
                                                   String dsState,
                                                   String logMessage)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String retval =
                    m_delegate.setDatastreamState(pid,
                                                  dsID,
                                                  dsState,
                                                  logMessage);
            flushIfNecessary();
            return retval;
        }

        public String setDatastreamVersionable(String pid,
                                               String dsID,
                                               boolean versionable,
                                               String logMessage)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String retval =
                    m_delegate.setDatastreamVersionable(pid,
                                                        dsID,
                                                        versionable,
                                                        logMessage);
            flushIfNecessary();
            return retval;
        }

        public String compareDatastreamChecksum(String pid,
                                                String dsID,
                                                String versionDate)
                throws java.rmi.RemoteException {
            return compareDatastreamChecksum(pid, dsID, versionDate);
        }

        public Datastream getDatastream(String pid,
                                        String dsID,
                                        String asOfDateTime)
                throws java.rmi.RemoteException {
            return m_delegate.getDatastream(pid, dsID, asOfDateTime);
        }

        public Datastream[] getDatastreams(String pid,
                                           String asOfDateTime,
                                           String dsState)
                throws java.rmi.RemoteException {
            return m_delegate.getDatastreams(pid, asOfDateTime, dsState);
        }

        public fedora.server.types.gen.Datastream[] getDatastreamHistory(String pid,
                                                                         String dsID)
                throws java.rmi.RemoteException {
            return m_delegate.getDatastreamHistory(pid, dsID);
        }

        public String[] purgeDatastream(String pid,
                                        String dsID,
                                        String startDT,
                                        String endDT,
                                        String logMessage,
                                        boolean force)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            String[] retval =
                    m_delegate.purgeDatastream(pid,
                                               dsID,
                                               startDT,
                                               endDT,
                                               logMessage,
                                               force);
            flushIfNecessary();
            return retval;
        }

        public String[] getNextPID(org.apache.axis.types.NonNegativeInteger numPIDs,
                                   String pidNamespace)
                throws java.rmi.RemoteException {
            return m_delegate.getNextPID(numPIDs, pidNamespace);
        }

        public RelationshipTuple[] getRelationships(String pid,
                                                    String relationship)
                throws java.rmi.RemoteException {
            return m_delegate.getRelationships(pid, relationship);
        }

        public boolean addRelationship(String pid,
                                       String relationship,
                                       String object,
                                       boolean isLiteral,
                                       String datatype)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            boolean retval =
                    m_delegate.addRelationship(pid,
                                               relationship,
                                               object,
                                               isLiteral,
                                               datatype);
            flushIfNecessary();
            return retval;
        }

        public boolean purgeRelationship(String pid,
                                         String relationship,
                                         String object,
                                         boolean isLiteral,
                                         String datatype)
                throws java.rmi.RemoteException {
            checkDurability(pid);
            boolean retval =
                    m_delegate.purgeRelationship(pid,
                                                 relationship,
                                                 object,
                                                 isLiteral,
                                                 datatype);
            flushIfNecessary();
            return retval;
        }
    }
}
