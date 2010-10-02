package shards;

public class PreparedStatementParameter {
	private final String methodName;
	private final Class<?>[] methodParamsTypes;
	private final Object[] values;
	
	public PreparedStatementParameter(String methodName, Object value, Class<?> methodParamType) {
		this.methodName = methodName;
		this.values = new Object[] { value };
		this.methodParamsTypes = new Class<?>[] { methodParamType };
	}
	
	public PreparedStatementParameter(String methodName, Object[] values, Class<?>[] methodParamsTypes) {
	    this.methodName = methodName;
	    this.values = values;
	    this.methodParamsTypes = methodParamsTypes;
	}

	public String getMethodName() {
		return methodName;
	}
	
	public Class<?>[] getMethodParamsTypes() {
        return methodParamsTypes;
    }

	public Object getValue() {
		return values[0];
	}
	
	public Object[] getValues() {
        return values;
    }
	
}
