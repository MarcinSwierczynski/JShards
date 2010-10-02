package shards;

import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import shards.QueryVisitor.ParameterInfoBean;

public class JSqlParser implements SQLParser {

	public ParseResult parse(String sql, ShardsSelectionStrategy strategy) throws WrongShardsQueryException, SQLParserException {
		return parse(sql, strategy, new PreparedStatementParametersList());
	}
	
	public ParseResult parse(String sql, ShardsSelectionStrategy strategy, PreparedStatementParametersList preparedStatementParameters) throws WrongShardsQueryException, SQLParserException {
		CCJSqlParserManager pm = new CCJSqlParserManager();
		try {
			Statement statement = pm.parse(new StringReader(sql));
			QueryVisitor qv = new QueryVisitor(strategy, preparedStatementParameters);
			statement.accept(qv);
			ParseResult result = new ParseResult(qv);
			return result;
		} catch (JSQLParserException e) {
			throw new SQLParserException(e);
		} catch (WrongShardsQueryException e) {
			throw new SQLParserException(e.getMessage(), e);
		} catch (RuntimeException e) {
			Set<String> selectedShards = strategy.selectShards(new ParameterInfoBean());
			ParseResult result = new ParseResult(selectedShards, sql);
			result.setWarning(new SQLWarning(e.getMessage(), e));
			return result;
		}
	}

	public ShardsResultSet filter(String sql, ShardsResultSet resultSet, List<Integer> havingColumns) throws SQLParserException {
		CCJSqlParserManager pm = new CCJSqlParserManager();
		try {
			Statement statement = pm.parse(new StringReader(sql));
			ShardsResultSet results = new ShardsResultSet(resultSet.getMetaData());
			while (resultSet.next()) {
				Object[] row = resultSet.getCurrentRow().asArray();
				HavingExpressionVisitor havingVisitor = new HavingExpressionVisitor(row, havingColumns);
				statement.accept(havingVisitor);
				if (havingVisitor.rowAccepted()) {
					results.addRow(row);
				}
			}
			return results;
		} catch (JSQLParserException e) {
			throw new SQLParserException(e);
		} catch (SQLException e) {
			throw new SQLParserException(e);
		}
	}

}
