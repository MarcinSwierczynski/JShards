package shards;

public interface WhereExpressionFactory {
	WhereExpression parse(String sql, ShardsSelectionStrategy strategy);
}
