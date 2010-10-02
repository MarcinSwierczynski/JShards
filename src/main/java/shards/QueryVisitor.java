package shards;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ColumnIndex;
import net.sf.jsqlparser.statement.select.ColumnReference;
import net.sf.jsqlparser.statement.select.ColumnReferenceVisitor;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import utils.BasicExpressionVisitor;
import utils.Calculation;
import utils.ExpressionVisitorAdapter;
import utils.SetsUtils;
import utils.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class QueryVisitor implements StatementVisitor, ItemsListVisitor, ExpressionVisitor, SelectVisitor, OrderByVisitor, SelectItemVisitor, FromItemVisitor {
	private Stack<Object> resultStack = new Stack<Object>();
	private Calculation calculation = new Calculation();
	private ShardsSelectionStrategy shardsSelectionStrategy;

	private Stack<Set<String>> shardsStack = new Stack<Set<String>>();

	private boolean inversed = false;
	private boolean inWhere = false;
	private boolean hasHaving = false;

	private List<OrderByColumn> orderByColumns = Lists.newArrayList();
	private List<Integer> groupByColumns = Lists.newArrayList();
	private List<Integer> havingColumns = Lists.newArrayList();
	private List<AvgIndex> avgIndexes = Lists.newArrayList();
	
	private List<Integer> asteriskIndexes = Lists.newArrayList();

	private String rewrittenQuery;

	private List<shards.Function> aggregateFunctions = Lists.newArrayList();
	private int columnIndex;
	private List<SelectItem> selectItems;

	private int columnsAdded = 0;
	private int expressionListCounter = 0;
	private String tableName;
	private Operation operation;
	private PreparedStatementParametersList preparedStatementParameters;
	private int preparedStatementParameterIndex;
	private Long limit;
	private long offset;

	public QueryVisitor(ShardsSelectionStrategy strategy, PreparedStatementParametersList preparedStatementParameters) {
		this.shardsSelectionStrategy = strategy;
		this.preparedStatementParameters = preparedStatementParameters;
		this.preparedStatementParameterIndex = 1;
	}
	
	public boolean hasHaving() {
		return hasHaving;
	}
	
	public int getColumnCount() {
		if (selectItems != null) {
			return selectItems.size();
		} else {
			return 0;
		}
	}
	
	public int getOriginalColumnsCount() {
		return getColumnCount() - getColumnsAdded();
	}

	public Set<String> getSelectedShards() {
		if (shardsStack.size() > 0) {
			return shardsStack.pop();
		}
		ParameterInfoBean info = new ParameterInfoBean();
		info.setOperation(this.operation);
		info.setTable(this.tableName);
		return shardsSelectionStrategy.selectShards(info);
	}
	
	public List<Integer> getHavingColumns() {
		return havingColumns;
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
	
	public List<AvgIndex> getAvgIndexes() {
		return avgIndexes;
	}
	
	public List<Integer> getAsteriskIndexes() {
		return asteriskIndexes;
	}

	public List<shards.Function> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public int getColumnsAdded() {
		return columnsAdded;
	}

	public Long getLimit() {
		return limit;
	}

	public long getOffset() {
		return offset;
	}

	public void visit(Select select) {
		select.getSelectBody().accept(this);
	}

	public void visit(Delete delete) {
		tableName = delete.getTable().getWholeTableName();
		operation = Operation.DELETE;
		Expression where = delete.getWhere();
		if (where != null) {
			inWhere = true;
			where.accept(this);
		}
	}

	public void visit(Update update) {
		tableName = update.getTable().getWholeTableName();
		operation = Operation.UPDATE;
		
		increasePreparedStatementParameterIndexByQuestionMarksQuantity(update);
		
		Expression where = update.getWhere();
		if (where != null) {
			inWhere = true;
			where.accept(this);
		}
	}

    private void increasePreparedStatementParameterIndexByQuestionMarksQuantity(Update update) {
        List<Expression> expressions = update.getExpressions();
		for (Expression expression : expressions) {
            expression.accept(new ExpressionVisitorAdapter() {
                public void visit(JdbcParameter param) {
                    preparedStatementParameterIndex++;
                };
            });
        }
    }

	@SuppressWarnings("unchecked")
	public void visit(Insert insert) {
		tableName = insert.getTable().getWholeTableName();
		operation = Operation.INSERT;
		
		List<Column> columns = insert.getColumns();
		if (columns == null) {
			throw new RuntimeException("Columns have to be specified in INSERT statement");
		}

		ItemsList itemsList = insert.getItemsList();
		itemsList.accept(this);

		List<Set<String>> selectedShards = Lists.newArrayList();

		// get values from resultStack
		ParameterInfoBean[] params = new ParameterInfoBean[columns.size()];
		for (int i = columns.size() - 1; i >= 0; i--) {
			Object value = resultStack.pop();
			params[i] = new ParameterInfoBean(extractColumnName(columns.get(i)), tableName, operation);
			params[i].setValue(value);
			params[i].setOperator(Operator.EQUAL);
		}

		for (ParameterInfo param : params) {
			selectedShards.add(shardsSelectionStrategy.selectShards(param));
		}
		Set<String> interception = SetsUtils.interception(selectedShards);
		shardsStack.push(interception);
	}

	private Set<String> pushAllShards() {
		ParameterInfoBean info = new ParameterInfoBean();
		info.setOperation(Operation.DDL);
		info.setTable(tableName);
		Set<String> selectedShards = shardsSelectionStrategy.selectShards(info);
		return shardsStack.push(selectedShards);
	}

	public void visit(Replace replace) {
		pushAllShards();
	}

	public void visit(Drop drop) {
		pushAllShards();
	}

	public void visit(Truncate truncate) {
		pushAllShards();
	}

	public void visit(CreateTable table) {
		pushAllShards();
	}

	public void visit(SubSelect subSelect) {
		throw new WrongShardsQueryException("Subselects cannot be used in shards architecture.");
	}

	@SuppressWarnings("unchecked")
	public void visit(ExpressionList expList) {
		List<Expression> expressions = expList.getExpressions();
		for (Expression expression : expressions) {
			expression.accept(this);
			expressionListCounter++;
		}
	}

	public void visit(NullValue arg0) {
		resultStack.push(null);
	}

	public void visit(Function function) {
		function.getParameters().accept(this);
	}

	public void visit(InverseExpression inverse) {
		inversed = true;
		inverse.getExpression().accept(this);
	}

	public void visit(JdbcParameter param) {
	    Object value = getNextPrepareStatementParameter();
	    resultStack.push(value);
	}

	private Object getNextPrepareStatementParameter() {
		PreparedStatementParameter parameter = preparedStatementParameters.get(preparedStatementParameterIndex);
	    Object value = null;
	    if (parameter != null) {
	        value = parameter.getValue();
	    }
	    
	    value = convertToProperType(value);
	    preparedStatementParameterIndex++;
		return value;
	}
	
	private Object convertToProperType(Object value) {
	    if (value != null) {
	        if (value instanceof Number) {
	            if (value instanceof Byte) {
	                Byte number = (Byte) value;
	                value = number.longValue();
	            } else if (value instanceof Short) {
	                Short number = (Short) value;
	                value = number.longValue();
	            } else if (value instanceof Integer) {
                    Integer number = (Integer) value;
                    value = number.longValue();
	            } else if (value instanceof Float) {
	                Float number = (Float) value;
	                value = number.doubleValue();
	            } 
	        }
	    }
		return value;
	}

	public void visit(DoubleValue doubleVal) {
		double val = doubleVal.getValue();
		if (inversed) {
			val = -val;
			inversed = false;
		}
		resultStack.push(val);
	}

	public void visit(LongValue longVal) {
		long val = longVal.getValue();
		if (inversed) {
			val = -val;
			inversed = false;
		}
		resultStack.push(val);
	}

	public void visit(DateValue dateValue) {
		resultStack.push(dateValue.getValue());
	}

	public void visit(TimeValue timeValue) {
		resultStack.push(timeValue.getValue());
	}

	public void visit(TimestampValue timeStampValue) {
		resultStack.push(timeStampValue.getValue());
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	public void visit(StringValue stringVal) {
		resultStack.push(stringVal.getValue());
	}

	interface CalculationCallback {
		Object calculate(Object arg1, Object arg2);
	}

	private void calculate(BinaryExpression exp, CalculationCallback callback) {
		exp.getLeftExpression().accept(this);
		exp.getRightExpression().accept(this);
		Object arg2 = resultStack.pop();
		Object arg1 = resultStack.pop();
		if (arg1 instanceof Column || arg2 instanceof Column) {
			throw new ColumnEvaluateException("Unable to evaluate expression on column(s): " + arg1 + " and " + arg2);
		}
		Object outcome = callback.calculate(arg1, arg2);
		resultStack.push(outcome);
	}

	public void visit(Addition exp) {
		calculate(exp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.add(arg1, arg2);
			}
		});
	}

	public void visit(Division exp) {
		calculate(exp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.divide(arg1, arg2);
			}
		});
	}

	public void visit(Multiplication exp) {
		calculate(exp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.multiply(arg1, arg2);
			}
		});
	}

	public void visit(Subtraction exp) {
		calculate(exp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.subtract(arg1, arg2);
			}
		});
	}

	public void visit(AndExpression andExp) {
		andExp.getLeftExpression().accept(this);
		andExp.getRightExpression().accept(this);
		Set<String> set1 = shardsStack.pop();
		Set<String> set2 = shardsStack.pop();
		shardsStack.push(SetsUtils.interception(set1, set2));
	}

	public void visit(OrExpression orExp) {
		orExp.getLeftExpression().accept(this);
		orExp.getRightExpression().accept(this);
		Set<String> set1 = shardsStack.pop();
		Set<String> set2 = shardsStack.pop();
		shardsStack.push(SetsUtils.sum(set1, set2));
	}

	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);

		Object end = resultStack.pop();
		Object start = resultStack.pop();
		Object left = resultStack.pop();

		if (end instanceof Column || start instanceof Column) {
			throw new ColumnEvaluateException("The right side of BETWEEN expression cannot contain a column: " + left + " BETWEEN " + start + " AND " + end);
		}

		if (left instanceof Column) {
			Column column = (Column) left;

			ParameterInfoBean paramInfo = new ParameterInfoBean(extractColumnName(column), tableName, operation);
			paramInfo.setOperator(Operator.MORE_EQUAL);
			paramInfo.setValue(start);
			Set<String> startShards = shardsSelectionStrategy.selectShards(paramInfo);
			
			paramInfo = new ParameterInfoBean(extractColumnName(column), tableName, operation);
			paramInfo.setOperator(Operator.LESS_EQUAL);
			paramInfo.setValue(end);
			Set<String> endShards = shardsSelectionStrategy.selectShards(paramInfo);

			Set<String> interception = SetsUtils.interception(startShards, endShards);
			shardsStack.push(interception);
		}
	}

	public void visit(EqualsTo arg0) {
		visitBinaryExpression(arg0, Operator.EQUAL);
	}

	public void visit(GreaterThan arg0) {
		visitBinaryExpression(arg0, Operator.MORE);
	}

	private void visitBinaryExpression(BinaryExpression exp, Operator operator) {
		exp.getLeftExpression().accept(this);
		exp.getRightExpression().accept(this);
		Object arg1 = resultStack.pop();
		Object arg2 = resultStack.pop();

		if (arg1 instanceof Column && arg2 instanceof Column) {
			throw new ColumnEvaluateException("Cannot evaluate expression with column on both sides: " + arg1 + " and " + arg2);
		}

		if (arg1 instanceof Column || arg2 instanceof Column) {
			Column column = null;
			Object value = null;
			if (arg1 instanceof Column) {
				column = (Column) arg1;
				value = arg2;
			} else {
				column = (Column) arg2;
				value = arg1;
			}
			
			ParameterInfoBean paramInfo = new ParameterInfoBean(extractColumnName(column), tableName, operation);
			paramInfo.setOperator(operator);
			paramInfo.setValue(value);
			shardsStack.push(shardsSelectionStrategy.selectShards(paramInfo));
		}

	}

	public void visit(GreaterThanEquals arg0) {
		visitBinaryExpression(arg0, Operator.MORE_EQUAL);
	}

	public void visit(InExpression in) {
		in.getLeftExpression().accept(this);
		Object left = resultStack.pop();

		if (left instanceof Column) {
			Column column = (Column) left;
			in.getItemsList().accept(this);
			List<Set<String>> shards = Lists.newArrayList();

			// get values from resultStack
			while (expressionListCounter > 0) {
				Object value = resultStack.pop();
				ParameterInfoBean paramInfo = new ParameterInfoBean(extractColumnName(column), tableName, operation);
				paramInfo.setOperator((in.isNot() ? Operator.NOT_EQUAL : Operator.EQUAL));
				paramInfo.setValue(value);
				shards.add(shardsSelectionStrategy.selectShards(paramInfo));
				expressionListCounter--;
			}
			if (in.isNot()) {
				shardsStack.push(SetsUtils.interception(shards));
			} else {
				shardsStack.push(SetsUtils.sum(shards));
			}

		}
	}

	public void visit(IsNullExpression isNull) {
		isNull.getLeftExpression().accept(this);
		Object left = resultStack.pop();
		if (left instanceof Column) {
			Column column = (Column) left;
			ParameterInfoBean param = new ParameterInfoBean(extractColumnName(column), tableName, operation);
			param.setOperator(isNull.isNot() ? Operator.NOT_EQUAL : Operator.EQUAL);
			param.setValue(null);
			Set<String> shards = shardsSelectionStrategy.selectShards(param);
			shardsStack.push(shards);
		}
	}

	public void visit(LikeExpression like) {
		visitBinaryExpression(like, Operator.LIKE);
	}

	public void visit(MinorThan arg0) {
		visitBinaryExpression(arg0, Operator.LESS);
	}

	public void visit(MinorThanEquals arg0) {
		visitBinaryExpression(arg0, Operator.LESS_EQUAL);
	}

	public void visit(NotEqualsTo arg0) {
		visitBinaryExpression(arg0, Operator.NOT_EQUAL);
	}

	public void visit(Column column) {
		resultStack.push(column);
	}

	public void visit(CaseExpression caseExp) {
		if (inWhere) {
			throw new RuntimeException("CASE expression cannot be used in WHERE clause in shards architecture");
		}
	}

	public void visit(WhenClause when) {
		if (inWhere) {
			throw new RuntimeException("WHEN clause cannot be used in WHERE clause in shards architecture");
		}
	}

	public void visit(AnyComparisonExpression any) {
		// Executing subselects in Shards architecture is not possible 
		// unless you execute subselects indepedently and pass results 
		// of these subselects to the main query i.e.
		// select * from shards where id = ANY (SELECT 1 UNION SELECT 2)
		// instead of
		// select * from shards where id = ANY (SELECT id from shards where id < 3)
		// But this solution will not work if the results will be to big (the 
		// rewritten query will have to much characters)
		throw new WrongShardsQueryException("Subselects cannot be used in shards architecture.");
	}

	public void visit(AllComparisonExpression all) {
		throw new WrongShardsQueryException("Subselects cannot be used in shards architecture.");
	}

	public void visit(ExistsExpression exists) {
		throw new WrongShardsQueryException("Subselects cannot be used in shards architecture.");
	}

	public void visit(PlainSelect select) {
		select.getFromItem().accept(this);
		operation = Operation.SELECT;
		
		selectItems = select.getSelectItems();
		columnIndex = 0;
		for (SelectItem selectItem : Lists.newArrayList(selectItems)) {
			columnIndex++;
			selectItem.accept(this);
		}
		columnIndex += columnsAdded;

		Expression where = select.getWhere();
		if (where != null) {
			inWhere = true;
			where.accept(this);
		}
		List<OrderByElement> orderByElements = select.getOrderByElements();
		if (orderByElements != null) {
			for (OrderByElement orderByElement : orderByElements) {
				orderByElement.accept(this);
			}
		}
		List<ColumnReference> groupByColumnRefs = select.getGroupByColumnReferences();
		if (groupByColumnRefs != null) {
			for (ColumnReference columnRef : groupByColumnRefs) {
				columnRef.accept(new ColumnReferenceVisitor() {
					
					public void visit(Column column) {
						String columnName = extractColumnName(column);
						addSelectItem(columnName);
						columnIndex++;
						groupByColumns.add(columnIndex);
					}
					
					public void visit(ColumnIndex columnIndex) {
						groupByColumns.add(columnIndex.getIndex());
					}
				});
			}
		}
		Expression having = select.getHaving();
		if (having != null) {
			hasHaving = true;
			having.accept(new BasicExpressionVisitor() {
				public void visit(Function function) {
					SelectExpressionItem selectItem = new SelectExpressionItem();
					selectItem.setExpression(function);
					selectItems.add(selectItem);
					columnIndex++;
					columnsAdded++;
					havingColumns.add(columnIndex);
					selectItem.accept(QueryVisitor.this);
				}
				public void visit(Column column) {
					String columnName = extractColumnName(column);
					addSelectItem(columnName);
					columnIndex++;
					havingColumns.add(columnIndex);
				}
			});
			select.setHaving(null);
		}
		
		manageLimit(select);
		
		rewrittenQuery = select.toString();
	}

	private void manageLimit(PlainSelect select) {
		Limit limit = select.getLimit();
		if(limit != null) {
			setLimit(limit);
			setOffset(limit);
		}
	}
	
	private void setOffset(Limit limit) {
		if(limit.isOffsetJdbcParameter()) {
			this.offset = getLimitOrOffsetAsLong();
		} else {
			this.offset = limit.getOffset();
		}
		//We can't pass offset to shards
		limit.setOffset(0);
	}

	private void setLimit(Limit limit) {
		if (limit.isRowCountJdbcParameter()) {
			this.limit = getLimitOrOffsetAsLong();
		} else if (limit.isLimitAll()) {
			this.limit = null;
		} else {
			this.limit = limit.getRowCount();
		}
		
		//Have to increase limit of offset value before passing it to shards
		if(this.limit != null) {
			limit.setRowCount(this.limit + limit.getOffset());
		}
	}

	private long getLimitOrOffsetAsLong() {
		Object prepareStatementLimit = getNextPrepareStatementParameter();
		if (prepareStatementLimit instanceof Number) {
			Number numberValue = (Number) prepareStatementLimit;
			return numberValue.longValue();
		} else {
			throw new WrongShardsQueryException("Limit and offset values have to be numbers.");
		}
	}

	private void addSelectItem(String columnName) {
		SelectItem columnSelectItem = createColumnSelectItem(columnName);
		selectItems.add(columnSelectItem);
		columnsAdded++;
	}

	private SelectItem createColumnSelectItem(String columnName) {
		SelectExpressionItem item = new SelectExpressionItem();
		Column column = new Column(new Table(), columnName);
		item.setExpression(column);
		return item;
	}

	@SuppressWarnings("unchecked")
	public void visit(Union union) {
		List<PlainSelect> selects = union.getPlainSelects();
		for (Object select : selects) {
			((PlainSelect) select).accept(this);
			List<Set<String>> shardsList = Lists.newArrayList();
			while (!shardsStack.isEmpty()) {
				shardsList.add(shardsStack.pop());
			}
			shardsStack.push(SetsUtils.sum(shardsList));
		}
	}

	public static class ParameterInfoBean implements ParameterInfo {
		private String column;
		private Operator operator;
		private Type type;
		private Object value;
		private String table;
		private Operation operation;

		public Operation getOperation() {
			return operation;
		}

		public void setOperation(Operation operation) {
			this.operation = operation;
		}

		public ParameterInfoBean() {
		}

		public ParameterInfoBean(String column, String table, Operation operation) {
			this.column = column;
			this.table = table;
			this.operation = operation;
		}

		public String getColumn() {
			return column;
		}

		public Operator getOperator() {
			return operator;
		}

		public Type getType() {
			return type;
		}

		public Object getValue() {
			return value;
		}

		public double getValueAsDouble() {
			if (value instanceof Double) {
				return (Double) value;
			} else if (value instanceof Long) {
				return ((Long) value).doubleValue();
			}
			throw new WrongShardsQueryException("Given value " + value + " is not a number.");
		}

		public void setColumn(String column) {
			this.column = column;
		}

		public void setOperator(Operator operator) {
			this.operator = operator;
		}

		public void setValue(Object value) {
			type = Type.fromValue(value);
			this.value = value;
		}
		
		public void setTable(String table) {
			this.table = table;
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", column, operator, value);
		}

		public String getTable() {
			return this.table;
		}

	}

	public void visit(OrderByElement orderByElement) {
		final boolean ascending = orderByElement.isAsc();
		orderByElement.getColumnReference().accept(new ColumnReferenceVisitor() {

			public void visit(Column column) {
				String columnName = extractColumnName(column);
				addSelectItem(columnName);
				columnIndex++;
				QueryVisitor.this.orderByColumns.add(new OrderByColumn(columnIndex, ascending));
			}

			public void visit(ColumnIndex columnIdx) {
				QueryVisitor.this.orderByColumns.add(new OrderByColumn(columnIdx.getIndex(), ascending));
			}
		});
	}

	public void visit(AllColumns allColumns) {
		asteriskIndexes.add(columnIndex);
	}

	public void visit(AllTableColumns allTableColumns) {
	}

	public void visit(SelectExpressionItem selectExpressionItem) {
		selectExpressionItem.getExpression().accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Function function) {
				String name = function.getName();
				FunctionType type = FunctionType.fromValue(name);
				if (type != null) {
					if (!type.equals(FunctionType.AVG)) {
						shards.Function aggregateFunction = new shards.Function(type, columnIndex);
						aggregateFunctions.add(aggregateFunction);
					} else {
						Function sumFunction = new Function();
						sumFunction.setName("sum");
						sumFunction.setParameters(function.getParameters());
						SelectExpressionItem sumSelectItem = new SelectExpressionItem();
						sumSelectItem.setExpression(sumFunction);
						selectItems.add(sumSelectItem);
						//Set avg function index
						avgIndexes.add(new AvgIndex(columnIndex, selectItems.size()));
						columnsAdded++;
						
						shards.Function sum = new shards.Function(FunctionType.SUM, selectItems.size());
						aggregateFunctions.add(sum);
						
						Function countFunction = new Function();
						countFunction.setName("count");
						countFunction.setParameters(function.getParameters());
						SelectExpressionItem countSelectItem = new SelectExpressionItem();
						countSelectItem.setExpression(countFunction);
						selectItems.add(countSelectItem);
						columnsAdded++;
						
						shards.Function count = new shards.Function(FunctionType.COUNT, selectItems.size());
						aggregateFunctions.add(count);
					}
				}
			}
		});
	}
	
	private String extractColumnName(Column column) {
		String cName = column.getColumnName();
		String stripped = StringUtils.stripLeadingAndTrailingQuotes(cName);
		return stripped;
	}

	public void visit(Table table) {
		tableName = table.getWholeTableName();
	}

	public void visit(SubJoin subJoin) {
		// TODO Auto-generated method stub
		
	}
}
