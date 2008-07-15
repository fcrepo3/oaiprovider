
package fedora.services.oaiprovider;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.provider.SQLProvider;
import org.nsdl.mptstore.rdf.Node;

import fedora.client.FedoraClient;
import fedora.common.PID;

import proai.SetInfo;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

public class MPTSetInfoIterator
        implements RemoteIterator<SetInfo> {

    private final FedoraClient client;

    private final MPTResultSetsManager results;

    private final int setObjectIndex;

    private final int setSpecIndex;

    private final int setNameIndex;

    private final int dissTargetIndex;

    private final InvocationSpec m_setInfoSpec;

    public MPTSetInfoIterator(FedoraClient client,
                              SQLProvider queryEngine,
                              DataSource d,
                              String disseminationTarget,
                              InvocationSpec setInfoSpec) {
        this.client = client;
        try {
            results = new MPTResultSetsManager(d, queryEngine);
        } catch (QueryException e) {
            throw new RepositoryException("Could not generate results query", e);
        }

        this.setObjectIndex = queryEngine.getTargets().indexOf("$set");
        if (setObjectIndex == -1) {
            throw new RuntimeException("$set not defined");
        }

        this.setSpecIndex = queryEngine.getTargets().indexOf("$setSpec");
        if (setSpecIndex == -1) {
            throw new RuntimeException("$setSpec not defined");
        }

        this.setNameIndex = queryEngine.getTargets().indexOf("$setName");
        if (setNameIndex == -1) {
            throw new RuntimeException("$setName not defined");
        }

        if (disseminationTarget != null) {
            this.dissTargetIndex =
                    queryEngine.getTargets().indexOf(disseminationTarget);
        } else {
            dissTargetIndex = -1;
        }

        m_setInfoSpec = setInfoSpec;
    }

    public void close() throws RepositoryException {
        try {
            results.close();
        } catch (SQLException e) {
            throw new RepositoryException("Could not close result set", e);
        }
    }

    public boolean hasNext() throws RepositoryException {
        return results.hasNext();
    }

    public SetInfo next() throws RepositoryException {
        try {
            if (results.hasNext()) {
                List<Node> result = results.next();

                Node setObjectResult = result.get(setObjectIndex);
                PID setObject = PID.getInstance(setObjectResult.getValue());

                String setSpec = null;
                Node setSpecResult = result.get(setSpecIndex);
                if (setSpecResult != null) {
                    setSpec = setSpecResult.getValue();
                }

                String setName = null;
                Node setNameResult = ((Node) result.get(setNameIndex));

                if (setNameResult != null) {
                    setName = setNameResult.getValue();
                }

                String setDiss = null;
                String setDissType = null;

                if (dissTargetIndex != -1) {
                    Node dissTargetResult = result.get(dissTargetIndex);
                    if (dissTargetResult != null) {
                        setDiss = dissTargetResult.getValue();
                    }
                }

                if (m_setInfoSpec != null) {
                    setDissType = m_setInfoSpec.getDisseminationType();
                }

                return new FedoraSetInfo(client,
                                         setObject.toString(),
                                         setSpec,
                                         setName,
                                         setDissType,
                                         setDiss);
            } else {
                throw new RepositoryException("No more results available\n");
            }
        } catch (SQLException e) {
            throw new RepositoryException("Could not read record result", e);
        }
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("remove() not supported");
    }

}
