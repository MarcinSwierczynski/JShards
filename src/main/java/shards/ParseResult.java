package shards;

import java.sql.SQLWarning;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

public class ParseResult {
	private final Set<String> selectedShards;
	private final List<OrderByColumn> orderByColumns;
	private final List<Integer> groupByColumns;
	private final List<Integer> havingColumns;
	private final String rewrittenQuery;
	private final List<Function> aggregateFunctions;
	private final List<AvgIndex> avgIndexes;
	private final List<Integer> asteriskIndexes;
	private final int columnsAdded;
	private final int originalColumnsCount;
	private final boolean hasHaving;
	private Long limit;
	private long offset;
	private SQLWarning warning;

	public ParseResult(QueryVisitor qv) {
		this.selectedShards = qv.getSelectedShards();
		this.orderByColumns = qv.getOrderByColumns();
		this.groupByColumns = qv.getGroupByColumns();
		this.rewrittenQuery = qv.getRewrittenQuery();
		this.aggregateFunctions = qv.getAggregateFunctions();
		this.avgIndexes = qv.getAvgIndexes();
		this.columnsAdded = qv.getColumnsAdded();
		this.asteriskIndexes = qv.getAsteriskIndexes();
		this.originalColumnsCount = qv.getOriginalColumnsCount();
		this.havingColumns = qv.getHavingColumns();
		this.hasHaving = qv.hasHaving();
		this.limit = qv.getLimit();
		this.offset = qv.getOffset();
	}
	
	public ParseResult(Set<String> selectedShards, String originalQuery) {
		this.selectedShards = selectedShards;
		this.orderByColumns = Lists.newArrayList();
		this.groupByColumns = Lists.newArrayList();
		this.rewrittenQuery = originalQuery;
		this.aggregateFunctions = Lists.newArrayList();
		this.avgIndexes = Lists.newArrayList(); 
		this.asteriskIndexes = Lists.newArrayList();
		this.columnsAdded = 0;
		this.originalColumnsCount = 0;
		this.havingColumns = Lists.newArrayList();
		this.hasHaving = false;
		this.limit = null;
		this.offset = 0;
	}
	
	public boolean hasHaving() {
		return hasHaving;
	}
	
	public List<Integer> getHavingColumns() {
		return havingColumns;
	}
	
	public Set<String> getSelectedShards() {
		return selectedShards;
	}

	public List<OrderByColumn> getOrderByColumns() {
		return orderByColumns;
	}

	public List<Integer> getGroupByColumns() {
		return groupByColumns;
	}

	public String getRewrittenQuery() {
		return rewrittenQuery;
	}

	public List<Function> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public List<AvgIndex> getAvgIndexes() {
		return avgIndexes;
	}
	
	public List<Integer> getAsteriskIndexes() {
		return asteriskIndexes;
	}

	public int getColumnsAdded() {
		return columnsAdded;
	}
	
	public int getOriginalColumnsCount() {
		return originalColumnsCount;
	}

	public Long getLimit() {
		return limit;
	}

	public long getOffset() {
		return offset;
	}

	public SQLWarning getWarning() {
		return warning;
	}
	
	public void setWarning(SQLWarning warning) {
		this.warning = warning;
	}
}
