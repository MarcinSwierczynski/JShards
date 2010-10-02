package shards;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ShardsStatementHoldability extends ShardsStatement {

    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;

    public ShardsStatementHoldability(ShardsConnection connection, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        super(connection);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    protected Statement internalCreateStatement(Connection connection) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

}
