package utils;

import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
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
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

/**
 * Prosta implementacja, która próbuje zejść najgłębiej ;)
 */
public class BasicExpressionVisitor extends ExpressionVisitorAdapter implements ItemsListVisitor {

	public void visit(Function function) {
		function.getParameters().accept(this);
	}

	public void visit(InverseExpression inverse) {
		inverse.getExpression().accept(this);
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
	
	protected void visitBinaryExpression(BinaryExpression exp) {
		exp.getLeftExpression().accept(this);
		exp.getRightExpression().accept(this);
	}

	public void visit(Addition add) {
		visitBinaryExpression(add);
	}

	public void visit(Division div) {
		visitBinaryExpression(div);
	}

	public void visit(Multiplication multi) {
		visitBinaryExpression(multi);
	}

	public void visit(Subtraction sub) {
		visitBinaryExpression(sub);
	}

	public void visit(AndExpression and) {
		visitBinaryExpression(and);
	}

	public void visit(OrExpression or) {
		visitBinaryExpression(or);
	}

	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	public void visit(EqualsTo equals) {
		visitBinaryExpression(equals);
	}

	public void visit(GreaterThan gt) {
		visitBinaryExpression(gt);
	}

	public void visit(GreaterThanEquals gte) {
		visitBinaryExpression(gte);		
	}

	public void visit(InExpression in) {
		in.getLeftExpression().accept(this);
		in.getItemsList().accept(this);
	}

	public void visit(IsNullExpression isNull) {
		isNull.getLeftExpression().accept(this);
	}

	public void visit(LikeExpression like) {
		visitBinaryExpression(like);		
	}

	public void visit(MinorThan mt) {
		visitBinaryExpression(mt);		
	}

	public void visit(MinorThanEquals mte) {
		visitBinaryExpression(mte);		
	}

	public void visit(NotEqualsTo notEquals) {
		visitBinaryExpression(notEquals);		
	}

	public void visit(WhenClause when) {
		when.getWhenExpression().accept(this);
		when.getThenExpression().accept(this);
	}

	public void visit(ExistsExpression exists) {
		exists.getRightExpression().accept(this);
	}

	public void visit(ExpressionList arg0) {
		List<Expression> expressions = arg0.getExpressions();
		for (Expression expression : expressions) {
			expression.accept(this);
		}
	}

}
