package shards;

public class Function {

	private final FunctionType type;
	private final int columnIndex;

	public Function(FunctionType type, int columnIndex) {
		this.type = type;
		this.columnIndex = columnIndex;
	}

	public FunctionType getType() {
		return type;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public Object aggregate(Object val1, Object val2) {
		return type.aggregate(val1, val2);
	}

}
