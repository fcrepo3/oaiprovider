
package fedora.services.oaiprovider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.provider.SQLProvider;
import org.nsdl.mptstore.rdf.Node;

import fedora.common.Constants;
import fedora.common.PID;

import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

public class MPTItemIterator
        implements RemoteIterator<FedoraRecord>, Constants {

    private static final Logger logger =
            Logger.getLogger(MPTItemIterator.class.getName());

    private final MPTResultSetsManager results;

    private final FedoraMetadataFormat format;

    private final int itemIndex;

    private final int itemIDIndex;

    private final int stateIndex;

    private final int dateIndex;

    private final int setSpecIndex;

    private final int aboutDissIndex;

    public MPTItemIterator(SQLProvider queryEngine,
                           DataSource d,
                           FedoraMetadataFormat format,
                           String aboutDissTarget) {

        try {
            results = new MPTResultSetsManager(d, queryEngine);
        } catch (QueryException e) {
            throw new RepositoryException("Could not generate results query", e);
        }

        this.format = format;

        this.itemIDIndex = queryEngine.getTargets().indexOf("$itemID");
        if (itemIDIndex == -1) {
            throw new RuntimeException("$itemID not defined");
        }

        this.stateIndex = queryEngine.getTargets().indexOf("$state");
        if (stateIndex == -1) {
            throw new RuntimeException("stateIndex is not defined");
        }

        this.dateIndex = queryEngine.getTargets().indexOf("$date");
        if (dateIndex == -1) {
            throw new RuntimeException("dateIndex is not defined");
        }

        this.itemIndex = queryEngine.getTargets().indexOf("$item");
        if (itemIndex == -1) {
            throw new RuntimeException("itemIndex is not defined");
        }

        this.setSpecIndex = queryEngine.getTargets().indexOf("$setSpec");

        if (aboutDissTarget != null) {
            this.aboutDissIndex =
                    queryEngine.getTargets().indexOf(aboutDissTarget);
        } else {
            aboutDissIndex = -1;
        }
    }

    public void close() throws RepositoryException {
        try {
            results.close();
        } catch (SQLException e) {
            throw new RepositoryException("Could not close result set", e);
        }
    }

    public boolean hasNext() throws RepositoryException {
        return (results.hasNext());
    }

    public FedoraRecord next() throws RepositoryException {
        try {
            if (results.hasNext()) {
                List<Node> result = results.next();

                PID pid = PID.getInstance(result.get(itemIndex).getValue());
                String itemID = (result.get(itemIDIndex)).getValue();

                String date = formatDate((result.get(dateIndex)).toString());
                boolean deleted =
                        !((result.get(stateIndex))).getValue()
                                .equals(MODEL.ACTIVE.uri);

                InvocationSpec mdSpec = format.getMetadataSpec();
                InvocationSpec aboutSpec = format.getAboutSpec();

                String recordDiss = "";

                recordDiss = mdSpec.getDisseminationType(pid);

                String aboutDiss = null;

                if (aboutSpec != null && aboutDissIndex != -1) {
                    if (result.get(aboutDissIndex) != null) {
                        aboutDiss = aboutSpec.getDisseminationType(pid);
                    }
                }

                /*
                 * Build a set of setSpecs. This assumes that the results are
                 * grouped by itemID
                 */
                Set<String> setSpecs = new HashSet<String>();
                if (setSpecIndex != -1) {
                    Node setSpecResult = ((Node) result.get(setSpecIndex));
                    if (setSpecResult != null) {
                        setSpecs.add(setSpecResult.getValue());
                    }
                    if (results.peek() != null) {
                        while (results.peek().get(itemIDIndex).getValue()
                                .equals(itemID)) {
                            List<Node> nextEntry = results.next();
                            setSpecResult = nextEntry.get(setSpecIndex);

                            if (setSpecResult != null) {
                                setSpecs.add(setSpecResult.getValue());
                            }
                            if (results.peek() == null) {
                                break;
                            }
                        }
                    }
                }

                String[] specs =
                        new ArrayList<String>(setSpecs)
                                .toArray(new String[setSpecs.size()]);

                return new FedoraRecord(itemID,
                                        format.getPrefix(),
                                        recordDiss,
                                        date,
                                        deleted,
                                        specs,
                                        aboutDiss);
            } else {
                throw new RepositoryException("No more results available\n");
            }
        } catch (SQLException e) {
            logger.error("Could not read recors result", e);
            throw new RepositoryException("Could not read record result", e);
        }
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("remove() not supported");
    }

    /*
     * expects date of the form
     * "2006-06-14T17:43:22"^^http://www.w3.org/2001/XMLSchema#dateTime
     */
    private static String formatDate(String tripleDate)
            throws RepositoryException {

        if (!tripleDate.contains("http://www.w3.org/2001/XMLSchema#dateTime")) {
            throw new RepositoryException("Unknown date format, must be of form "
                    + "'\"YYYY-MM-DDTHH:MM:SS.sss\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
                    + " but instead was given " + tripleDate);
        }

        return tripleDate.replace("\"", "")
                .replace("^^<http://www.w3.org/2001/XMLSchema#dateTime>", "")
                .replaceFirst("\\.[0-9]+Z*", "")
                + "Z";
    }
}