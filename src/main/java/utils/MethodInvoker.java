package utils;

import java.lang.reflect.Method;

public class MethodInvoker {
    
	public static Object invoke(Object object, String methodName, Object... params) {
		Class<?>[] parameterTypes = new Class<?>[params.length];
		for (int i = 0; i < params.length; i++) {
			parameterTypes[i] = params[i].getClass();
		}
		return invokeWithParametersTypes(object, methodName, parameterTypes, params);
	}

    public static Object invokeWithParametersTypes(Object object, String methodName, Class<?>[] parameterTypes, Object... params) {
        try {
			Method method = object.getClass().getMethod(methodName, parameterTypes);
			Object outcome = method.invoke(object, params);
			return outcome;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
	
}
