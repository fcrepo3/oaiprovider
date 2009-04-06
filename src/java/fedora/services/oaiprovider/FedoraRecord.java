
package fedora.services.oaiprovider;

import org.apache.log4j.Logger;

import proai.Record;

/**
 * @author Edwin Shin
 * @author cwilper@cs.cornell.edu
 */
public class FedoraRecord
        implements Record {

    public static Logger logger =
            Logger.getLogger(FedoraRecord.class.getName());

    private String m_itemID;

    private String m_mdPrefix;

    private String m_sourceInfo;

    public FedoraRecord(String itemID,
                        String mdPrefix,
                        String recordDiss,
                        String date,
                        boolean deleted,
                        String[] setSpecs,
                        String aboutDiss) {

        m_itemID = itemID;
        m_mdPrefix = mdPrefix;

        StringBuffer buf = new StringBuffer();
        buf.append(recordDiss);
        buf.append(" " + aboutDiss);
        buf.append(" " + deleted);
        buf.append(" " + date);
        for (int i = 0; i < setSpecs.length; i++) {
            String setSpec = setSpecs[i].replace(' ', '_');
            buf.append(" " + setSpec);
        }
        m_sourceInfo = buf.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see proai.Record#getItemID()
     */
    public String getItemID() {
        return m_itemID;
    }

    public String getPrefix() {
        return m_mdPrefix;
    }

    public String getSourceInfo() {
        logger.debug("Returning source info line: " + m_sourceInfo);
        return m_sourceInfo;
    }
}
