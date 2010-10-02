package shards;

public class OrderByColumn {

	private int columnIndex;
	private boolean ascending;

	public OrderByColumn(int columnIndex, boolean ascending) {
		this.columnIndex = columnIndex;
		this.ascending = ascending;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public boolean isAscending() {
		return ascending;
	}

	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	public void setAscending(boolean ascending) {
		this.ascending = ascending;
	}

}
