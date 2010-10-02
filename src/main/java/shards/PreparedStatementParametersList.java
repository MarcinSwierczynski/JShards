package shards;

import java.sql.PreparedStatement;
import java.util.Map;

import utils.MethodInvoker;

import com.google.common.collect.Maps;

public class PreparedStatementParametersList {
	Map<Integer, PreparedStatementParameter> parameters = Maps.newHashMap();

	public void addParameter(int parameterIndex, PreparedStatementParameter preparedStatementParameter) {
		parameters.put(parameterIndex, preparedStatementParameter);
	}

	public PreparedStatementParameter get(int parameterIndex) {
		return parameters.get(parameterIndex);
	}
	
	public void populate(PreparedStatement preparedStatement) {
	    for (Integer index : parameters.keySet()) {
	        PreparedStatementParameter parameter = parameters.get(index);
            Object value = parameter.getValue();
	        String methodName = parameter.getMethodName();
	        Class<?>[] parameterTypes = prepend(int.class, parameter.getMethodParamsTypes());
	        MethodInvoker.invokeWithParametersTypes(preparedStatement, methodName, parameterTypes, index, value);
	    }
	}
	
	public Class<?>[] prepend(Class<?> clazz, Class<?>[] original) {
        Class<?>[] result = new Class<?>[original.length + 1];
        result[0] = clazz;
        System.arraycopy(original, 0, result, 1, original.length);
        return result;
	}
	
	public void clear() {
	    this.parameters.clear();
	}

}
