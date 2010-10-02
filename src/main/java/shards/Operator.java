package shards;

public enum Operator {
	EQUAL("="), LESS("<"), MORE(">"), LESS_EQUAL("<="), MORE_EQUAL(">="), NOT_EQUAL("<>"), LIKE("LIKE"), NOT_LIKE("NOT LIKE");
	
	private String symbol;
	
	private Operator(String symbol) {
		this.symbol = symbol;
	}
	
	@Override
	public String toString() {
		return symbol;
	}
}
