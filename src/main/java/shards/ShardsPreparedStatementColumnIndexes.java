package shards;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ShardsPreparedStatementColumnIndexes extends ShardsPreparedStatement {

    private int[] columnIndexes;

    public ShardsPreparedStatementColumnIndexes(ShardsConnection connection, String sql, int[] columnIndexes) throws SQLException {
        super(connection, sql);
        this.columnIndexes = columnIndexes;
    }

    @Override
    protected PreparedStatement internalCreateStatement(Connection connection, String rewrittenQuery) throws SQLException {
        return connection.prepareStatement(rewrittenQuery, columnIndexes);
    }

}
