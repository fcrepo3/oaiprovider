
package fedora.services.oaiprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.nsdl.mptstore.query.QueryException;
import org.nsdl.mptstore.query.SQLUnionQueryResults;
import org.nsdl.mptstore.query.provider.SQLProvider;
import org.nsdl.mptstore.rdf.Node;

public class MPTResultSetsManager {

    private final SQLUnionQueryResults results;

    private List<Node> nextResult;

    public MPTResultSetsManager(DataSource dataSource, SQLProvider sqlSource)
            throws QueryException {
        try {
            Connection conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            this.results =
                    new SQLUnionQueryResults(conn, sqlSource, 1000, true);
            if (results.hasNext()) {
                this.nextResult = results.next();
            } else {
                this.nextResult = null;
            }
        } catch (SQLException e) {
            throw new QueryException("Error creating results", e);
        }
    }

    public List<Node> peek() throws SQLException {
        return this.nextResult;
    }

    public List<Node> next() throws SQLException {

        List<Node> currentResult = this.nextResult;
        if (results.hasNext()) {
            this.nextResult = results.next();
        } else {
            this.nextResult = null;
        }
        return currentResult;
    }

    public boolean hasNext() {
        return this.nextResult != null;
    }

    public void close() throws SQLException {
        results.close();
    }
}
