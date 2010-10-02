package shards;

import java.util.Arrays;
import java.util.List;

import utils.Assert;

public class Row {

	private Object[] rowData;

	public Row(Object[] rowData) {
		this.rowData = rowData;
	}

	protected Object[] asArray() {
		return rowData;
	}

	public boolean isEquals(Row row, List<OrderByColumn> groupByColumns) {
		Assert.notNull(row);
		Assert.notNull(groupByColumns);
		for (OrderByColumn column : groupByColumns) {
			int internalColumnIdx = column.getColumnIndex() - 1;
			Object val1 = rowData[internalColumnIdx];
			Object val2 = row.rowData[internalColumnIdx];
			if (!val1.equals(val2)) {
				return false;
			}
		}
		return true;
	}

	public Row aggregate(Row row, List<Function> functions) {
		Row result = new Row(Arrays.copyOf(asArray(), asArray().length));
		for (Function function : functions) {
			int columnIndex = function.getColumnIndex();
			int internalColumnIndex = columnIndex - 1;
			Object row1Val = rowData[internalColumnIndex];
			Object row2Val = row.rowData[internalColumnIndex];
			Object aggregateResult = function.aggregate(row1Val, row2Val);
			result.rowData[internalColumnIndex] = aggregateResult;
		}
		return result;
	}
	
	public synchronized void setColumnValue(int index, Object value) {
		int internalIdx = index - 1;
		rowData[internalIdx] = value;
	}
}
