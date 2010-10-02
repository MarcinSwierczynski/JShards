package shards;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ShardsStatementConcurrency extends ShardsStatement {

    private int resultSetType;
    private int resultSetConcurrency;

    public ShardsStatementConcurrency(ShardsConnection connection, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        super(connection);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    @Override
    protected Statement internalCreateStatement(Connection connection) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency);
    }

}