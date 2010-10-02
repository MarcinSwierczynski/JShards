package shards;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ShardsPreparedStatementConcurrency extends ShardsPreparedStatement {

    private int resultSetType;
    private int resultSetConcurrency;

    public ShardsPreparedStatementConcurrency(ShardsConnection connection, String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        super(connection, sql);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    @Override
    protected PreparedStatement internalCreateStatement(Connection connection, String rewrittenQuery) throws SQLException {
        return connection.prepareStatement(rewrittenQuery, resultSetType, resultSetConcurrency);
    }

}
