package shards;

public interface ParameterInfo {
	String getColumn();
	String getTable();
	Operator getOperator();
	Type getType();
	Object getValue();
	double getValueAsDouble();
	Operation getOperation();
}
