package shards;

import java.util.List;

public interface SQLParser {

	/** Parses SQL query and produce informations about selected shards and rewritten query */
	ParseResult parse(String sql, ShardsSelectionStrategy strategy) throws WrongShardsQueryException, SQLParserException;

	/** Parses parameterized SQL query and produce informations about selected shards and rewritten query */
	ParseResult parse(String sql, ShardsSelectionStrategy strategy, PreparedStatementParametersList preparedStatementParameters) throws WrongShardsQueryException, SQLParserException;

	/** Used for SQL queries containing HAVING clause to filter returned result set. */
	ShardsResultSet filter(String sql, ShardsResultSet resultSet, List<Integer> havingColumns) throws SQLParserException;

}
