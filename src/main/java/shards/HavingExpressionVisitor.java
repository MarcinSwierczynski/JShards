package shards;

import java.util.*;

import com.google.common.collect.Lists;

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
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import shards.QueryVisitor.ParameterInfoBean;
import utils.*;

public class HavingExpressionVisitor extends StatementVisitorAdapter implements SelectVisitor, ExpressionVisitor, ItemsListVisitor {

	private Stack<Object> resultStack = new Stack<Object>();
	private Calculation calculation = new Calculation();
	private boolean inversed = false;

	private final Object[] row;
	private final List<Integer> havingColumns;
	private int havingColumnsOffset = 0;
	private int expressionListCounter = 0;

	public HavingExpressionVisitor(Object[] row, List<Integer> havingColumns) {
		this.row = row;
		this.havingColumns = havingColumns;
	}

	public boolean rowAccepted() {
		Object result = resultStack.peek();
		if (result instanceof Boolean) {
			return (Boolean) result;
		} else {
			return false;
		}
	}

	public void visit(Select select) {
		select.getSelectBody().accept(this);
	}

	public void visit(PlainSelect plainSelect) {
		Expression having = plainSelect.getHaving();
		if (having != null) {
			having.accept(this);
		}
	}

	public void visit(Union arg0) {
	}

	public void visit(NullValue arg0) {
		resultStack.push(null);
	}

	public void visit(Function function) {
		Integer idx = havingColumns.get(havingColumnsOffset);
		resultStack.push(row[idx - 1]);
		havingColumnsOffset++;
	}

	public void visit(InverseExpression inverse) {
		inversed = true;
		inverse.getExpression().accept(this);
	}

	public void visit(JdbcParameter param) {
		// TODO Auto-generated method stub
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
		calculate(andExp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.and(arg1, arg2);
			}
		});
	}

	public void visit(OrExpression orExp) {
		calculate(orExp, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.or(arg1, arg2);
			}
		});
	}

	public void visit(Between between) {
		// >=
		shards.HavingExpressionVisitor.CalculationCallback callback = new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.greaterThanEquals(arg1, arg2);
			}
		};
		Object left = callback.calculate(between.getLeftExpression(), between.getBetweenExpressionStart());
		// <=
		callback = new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.minorThanEquals(arg1, arg2);
			}
		};
		Object right = callback.calculate(between.getLeftExpression(), between.getBetweenExpressionEnd());
		// AND
		callback = new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.and(arg1, arg2);
			}
		};
		callback.calculate(left, right);
	}

	public void visit(EqualsTo equalsTo) {
		calculate(equalsTo, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.equalsTo(arg1, arg2);
			}
		});
	}

	public void visit(GreaterThan arg0) {
		calculate(arg0, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.greaterThan(arg1, arg2);
			}
		});
	}

	public void visit(GreaterThanEquals arg0) {
		calculate(arg0, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.greaterThanEquals(arg1, arg2);
			}
		});
	}

	@SuppressWarnings("unchecked")
	public void visit(InExpression in) {
		in.getLeftExpression().accept(this);
		Object left = resultStack.pop();
		
		Comparable leftComparable = castToComparable(left);
		
		in.getItemsList().accept(this);
		
		boolean result = true;
		if (in.isNot()) {
			while (expressionListCounter > 0) {
				Object value = resultStack.pop();
				expressionListCounter--;
				Comparable<?> valueComparable = castToComparable(value);
				
				if(leftComparable.compareTo(valueComparable) == 0) {
					result = false;
				}
			}
		} else {
			result = false;
			// get values from resultStack
			while (expressionListCounter > 0) {
				Object value = resultStack.pop();
				expressionListCounter--;
				
				Comparable<?> valueComparable = castToComparable(value);
				
				if(leftComparable.compareTo(valueComparable) == 0) {
					result = true;
					while(expressionListCounter > 0) {
						resultStack.pop();
						expressionListCounter--;
					}
				}
			}
		}
		
		resultStack.push(result);
	}

	private Comparable<?> castToComparable(Object value) {
		Comparable<?> valueComparable;
		if (value instanceof Comparable) {
			valueComparable = (Comparable) value;
		} else {
			throw new RuntimeException("Cannot compare not-comparable values.");
		}
		return valueComparable;
	}

	public void visit(IsNullExpression isNull) {
		isNull.getLeftExpression().accept(this);
		Object result = resultStack.pop();
		resultStack.push(result == null);
	}

	public void visit(LikeExpression like) {
		like.getLeftExpression().accept(this);
		like.getRightExpression().accept(this);

		Object right = resultStack.pop();
		Object left = resultStack.pop();

		if (left == null || right == null) {
			resultStack.push(false);
		} else {
			String regexPattern = StringUtils.getRegexFromSqlLike(right.toString());
			boolean matches = left.toString().matches(regexPattern);
			resultStack.push(matches);
		}
	}

	public void visit(MinorThan minorThan) {
		calculate(minorThan, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.minorThan(arg1, arg2);
			}
		});
	}

	public void visit(MinorThanEquals minorThanEquals) {
		calculate(minorThanEquals, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.minorThanEquals(arg1, arg2);
			}
		});
	}

	public void visit(NotEqualsTo notEqualsTo) {
		calculate(notEqualsTo, new CalculationCallback() {
			public Object calculate(Object arg1, Object arg2) {
				return calculation.notEqualsTo(arg1, arg2);
			}
		});
	}

	public void visit(Column column) {
		Integer idx = havingColumns.get(havingColumnsOffset);
		resultStack.push(row[idx - 1]);
		havingColumnsOffset++;
	}

	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("Subselects are not supported in current version");
	}

	public void visit(CaseExpression caseExp) {
		throw new RuntimeException("Case clause is not supported within Having clause.");
	}

	public void visit(WhenClause when) {
		throw new RuntimeException("When clause is not supported within Having clause.");
	}

	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("Exists clause is not supported in current version");
	}

	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("All clause is not supported in current version");
	}

	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("Any clause is not supported in current version");
	}

	public void visit(ExpressionList expList) {
		List<Expression> expressions = expList.getExpressions();
		for (Expression expression : expressions) {
			expression.accept(this);
			expressionListCounter++;
		}
	}

}
