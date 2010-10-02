package shards;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ShardsPreparedStatementColumnNames extends ShardsPreparedStatement {

    private String[] columnNames;

    public ShardsPreparedStatementColumnNames(ShardsConnection connection, String sql, String[] columnNames) throws SQLException {
        super(connection, sql);
        this.columnNames = columnNames;
    }

    @Override
    protected PreparedStatement internalCreateStatement(Connection connection, String rewrittenQuery) throws SQLException {
        return connection.prepareStatement(rewrittenQuery, columnNames);
    }

}
