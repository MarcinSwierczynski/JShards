package shards;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ShardsPreparedStatementHoldability extends ShardsPreparedStatement {

    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;

    public ShardsPreparedStatementHoldability(ShardsConnection connection, String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        super(connection, sql);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    protected PreparedStatement internalCreateStatement(Connection connection, String rewrittenQuery) throws SQLException {
        return connection.prepareStatement(rewrittenQuery, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

}
