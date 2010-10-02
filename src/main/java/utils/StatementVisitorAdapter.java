package utils;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class StatementVisitorAdapter implements StatementVisitor {

	public void visit(Select arg0) {
	}

	public void visit(Delete arg0) {
	}

	public void visit(Update arg0) {
	}

	public void visit(Insert arg0) {
	}

	public void visit(Replace arg0) {
	}

	public void visit(Drop arg0) {
	}

	public void visit(Truncate arg0) {
	}

	public void visit(CreateTable arg0) {
	}

}
