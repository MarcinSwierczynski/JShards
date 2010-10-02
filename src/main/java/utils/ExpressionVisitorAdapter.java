package utils;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public abstract class ExpressionVisitorAdapter implements ExpressionVisitor {

	public void visit(NullValue arg0) {
	}

	public void visit(Function arg0) {
	}

	public void visit(InverseExpression arg0) {
	}

	public void visit(JdbcParameter arg0) {

	}

	public void visit(DoubleValue arg0) {

	}

	public void visit(LongValue arg0) {

	}

	public void visit(DateValue arg0) {

	}

	public void visit(TimeValue arg0) {

	}

	public void visit(TimestampValue arg0) {

	}

	public void visit(Parenthesis arg0) {

	}

	public void visit(StringValue arg0) {

	}

	public void visit(Addition arg0) {

	}

	public void visit(Division arg0) {

	}

	public void visit(Multiplication arg0) {

	}

	public void visit(Subtraction arg0) {

	}

	public void visit(AndExpression arg0) {

	}

	public void visit(OrExpression arg0) {

	}

	public void visit(Between arg0) {

	}

	public void visit(EqualsTo arg0) {

	}

	public void visit(GreaterThan gt) {
	}

	public void visit(GreaterThanEquals arg0) {
		
	}

	public void visit(InExpression arg0) {

	}

	public void visit(IsNullExpression arg0) {

	}

	public void visit(LikeExpression arg0) {

	}

	public void visit(MinorThan arg0) {

	}

	public void visit(MinorThanEquals arg0) {

	}

	public void visit(NotEqualsTo arg0) {

	}

	public void visit(Column arg0) {

	}

	public void visit(SubSelect arg0) {

	}

	public void visit(CaseExpression arg0) {

	}

	public void visit(WhenClause arg0) {

	}

	public void visit(ExistsExpression arg0) {

	}

	public void visit(AllComparisonExpression arg0) {

	}

	public void visit(AnyComparisonExpression arg0) {

	}

}
