package utils;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Dodać operacje na BigDecimal (wykorzystywane jest to w PreparedStatements)
// TODO Ponadto metody dodające liczby zmiennoprzecinkowe też powinny zwracac BigDecimal (wykorzystywac BigDecimal zamiast Double?)
public class Calculation {

	private static final Logger logger = LoggerFactory.getLogger(Calculation.class);
	
	public Object add(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "add", arg1, arg2);
		}
	}
	
	public static Object invoke(Object object, String methodName, Object arg1, Object arg2) {
        Class<?>[] parameterTypes = new Class<?>[2];
        parameterTypes[0] = arg1.getClass();
        parameterTypes[1] = arg2.getClass();
        try {
            Method method = object.getClass().getMethod(methodName, parameterTypes);
            Object outcome = method.invoke(object, new Object[] { arg1, arg2 });
            return outcome;
        } catch (NoSuchMethodException e) {
            String arg1ClassName = arg1.getClass().getSimpleName();
            String arg2ClassName = arg2.getClass().getSimpleName();
            throw new RuntimeException("Shards driver is not able to " + methodName + " values of types " + arg1ClassName + " and " + arg2ClassName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	public Object concat(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "concat", arg1, arg2);
		}
	}

	public Object subtract(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "subtract", arg1, arg2);
		}
	}

	public Object multiply(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "multiply", arg1, arg2);
		}
	}

	public Object divide(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "divide", arg1, arg2);
		}
	}
	
	public Object or(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "or", arg1, arg2);
		}
	}
	
	public Object and(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "and", arg1, arg2);
		}
	}
	
	public Object minorThan(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "minorThan", arg1, arg2);
		}
	}
	
	public Object minorThanEquals(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "minorThanEquals", arg1, arg2);
		}
	}
	
	public Object greaterThan(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "greaterThan", arg1, arg2);
		}
	}
	
	public Object greaterThanEquals(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "greaterThanEquals", arg1, arg2);
		}
	}
	
	public Object equalsTo(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "equalsTo", arg1, arg2);
		}
	}
	
	public Object notEqualsTo(Object arg1, Object arg2) {
		if (arg1 == null || arg2 == null) {
			return null;
		} else {
			return invoke(this, "notEqualsTo", arg1, arg2);
		}
	}

	// Adding '+'
	public Object add(BigDecimal a1, BigDecimal a2) {
		return a1.add(a2);
	}
	
	public Object add(Long a1, Long a2) {
		return a1 + a2;
	}
	
	public double add(Double a1, Double a2) {
		return a1 + a2;
	}

	public String add(String a1, String a2) {
		throw new RuntimeException("Cannot add (+) string to string. Use || instead");
	}

	public double add(Long a1, Double a2) {
		return a1 + a2;
	}

	public double add(Double a1, Long a2) {
		return add(a2, a1);
	}

	public long add(Long a1, String a2) {
		Long number = Types.castToLong(a2);
		if (number != null) {
			return a1 + number;
		} else {
			throw new RuntimeException("Cannot add (+) integer to string which does not hold a integer.");
		}
	}

	public long add(String a1, Long a2) {
		return add(a2, a1);
	}

	public double add(Double a1, String a2) {
		Double number = Types.castToDouble(a2);
		if (number != null) {
			return a1 + number;
		} else {
			throw new RuntimeException("Cannot add (+) integer to string which does not hold a number.");
		}
	}

	public double add(String a1, Double a2) {
		return add(a2, a1);
	}

	// concatenation '||'

	// TODO Na razie parser nie obsluguje konkatenacji!!!!
	public String concat(String a1, String a2) {
		return a1 + a2;
	}

	public long concat(Long a1, Long a2) {
		throw new RuntimeException("Cannot concatenate (||) integer to integer. Use + instead");
	}

	public double concat(Double a1, Double a2) {
		throw new RuntimeException("Cannot concatenate (||) numeric to numeric. Use + instead");
	}

	public double concat(Long a1, Double a2) {
		throw new RuntimeException("Cannot concatenate (||) integer to numeric. Use + instead");
	}

	public double concat(Double a1, Long a2) {
		return concat(a2, a1);
	}

	public String concat(Long a1, String a2) {
		return a1 + a2;
	}

	public String concat(String a1, Long a2) {
		return concat(a2, a1);
	}

	public String concat(Double a1, String a2) {
		return a1 + a2;
	}

	public String concat(String a1, Double a2) {
		return concat(a2, a1);
	}

	// subtract

	public String subtract(String a1, String a2) {
		throw new RuntimeException("Cannot subtract (-) string from string.");
	}

	public long subtract(Long a1, Long a2) {
		return a1 - a2;
	}

	public double subtract(Double a1, Double a2) {
		return a1 - a2;
	}

	public double subtract(Long a1, Double a2) {
		return a1 - a2;
	}

	public double subtract(Double a1, Long a2) {
		return a1 - a2;
	}

	public long subtract(Long a1, String a2) {
		Long number = Types.castToLong(a2);
		if (number != null) {
			return a1 - number;
		} else {
			throw new RuntimeException("String used in subtract (-) need to be an integer.");
		}
	}

	public long subtract(String a1, Long a2) {
		Long number = Types.castToLong(a1);
		if (number != null) {
			return number - a2;
		} else {
			throw new RuntimeException("String used in subtract (-) need to be an integer.");
		}
	}

	public double subtract(Double a1, String a2) {
		Double number = Types.castToDouble(a2);
		if (number != null) {
			return a1 - number;
		} else {
			throw new RuntimeException("String used in subtract (-) need to be a number.");
		}
	}

	public double subtract(String a1, Double a2) {
		Double number = Types.castToDouble(a1);
		if (number != null) {
			return number - a2;
		} else {
			throw new RuntimeException("String used in subtract (-) need to be a number.");
		}
	}

	// multiply

	public String multiply(String a1, String a2) {
		throw new RuntimeException("Cannot multiply (*) two strings.");
	}

	public long multiply(Long a1, Long a2) {
		return a1 * a2;
	}

	public double multiply(Double a1, Double a2) {
		return a1 * a2;
	}

	public double multiply(Long a1, Double a2) {
		return a1 * a2;
	}

	public double multiply(Double a1, Long a2) {
		return multiply(a2, a1);
	}

	public long multiply(Long a1, String a2) {
		Long number = Types.castToLong(a2);
		if (number != null) {
			return a1 * number;
		} else {
			throw new RuntimeException("String used in multiply (*) need to be an integer.");
		}
	}

	public long multiply(String a1, Long a2) {
		return multiply(a2, a1);
	}

	public double multiply(Double a1, String a2) {
		Double number = Types.castToDouble(a2);
		if (number != null) {
			return a1 - number;
		} else {
			throw new RuntimeException("String used in multiply (*) need to be a number.");
		}
	}

	public double multiply(String a1, Double a2) {
		return multiply(a2, a1);
	}

	// divide

	public String divide(String a1, String a2) {
		throw new RuntimeException("Cannot divide (/) two strings.");
	}

	public long divide(Long a1, Long a2) {
		return a1 / a2;
	}

	public double divide(Double a1, Double a2) {
		return a1 / a2;
	}

	public double divide(Long a1, Double a2) {
		return a1 / a2;
	}

	public double divide(Double a1, Long a2) {
		return a1 / a2;
	}

	public long divide(Long a1, String a2) {
		Long number = Types.castToLong(a2);
		if (number != null) {
			return a1 / number;
		} else {
			throw new RuntimeException("String used in divide (/) need to be an integer.");
		}
	}

	public long divide(String a1, Long a2) {
		Long number = Types.castToLong(a1);
		if (number != null) {
			return number / a2;
		} else {
			throw new RuntimeException("String used in divide (/) need to be an integer.");
		}
	}

	public double divide(Double a1, String a2) {
		Double number = Types.castToDouble(a2);
		if (number != null) {
			return a1 / number;
		} else {
			throw new RuntimeException("String used in divide (/) need to be a number.");
		}
	}

	public double divide(String a1, Double a2) {
		Double number = Types.castToDouble(a1);
		if (number != null) {
			return number / a2;
		} else {
			throw new RuntimeException("String used in divide (/) need to be a number.");
		}
	}

	// or

	public boolean or(Boolean a1, Boolean a2) {
		return a1 || a2;
	}
	
	// and 
	
	public boolean and(Boolean a1, Boolean a2) {
		return a1 && a2;
	}
	
	// minorThan
	
	public boolean minorThan(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) < 0;
	}

	public boolean minorThan(Long a1, Long a2) {
		return a1 < a2;
	}
	
	public boolean minorThan(Double a1, Double a2) {
		return a1 < a2;
	}
	
	public boolean minorThan(Long a1, Double a2) {
		return a1 < a2;
	}

	public boolean minorThan(Double a1, Long a2) {
		return a1 < a2;
	}

	public boolean minorThan(Long a1, String a2) {
		throw new RuntimeException("String used in minor than (<) need to be a number.");
	}
	
	public boolean minorThan(String a1, Long a2) {
		throw new RuntimeException("String used in minor than (<) need to be a number.");
	}

	public boolean minorThan(Double a1, String a2) {
		throw new RuntimeException("String used in minor than (<) need to be a number.");
	}

	public boolean minorThan(String a1, Double a2) {
		throw new RuntimeException("String used in minor than (<) need to be a number.");
	}
	
	// minorThanEquals
	
	public boolean minorThanEquals(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) <= 0;
	}

	public boolean minorThanEquals(Long a1, Long a2) {
		return a1 <= a2;
	}
	
	public boolean minorThanEquals(Double a1, Double a2) {
		return a1 <= a2;
	}
	
	public boolean minorThanEquals(Long a1, Double a2) {
		return a1 <= a2;
	}

	public boolean minorThanEquals(Double a1, Long a2) {
		return a1 <= a2;
	}

	public boolean minorThanEquals(Long a1, String a2) {
		throw new RuntimeException("String used in minor than equals (<=) need to be a number.");
	}
	
	public boolean minorThanEquals(String a1, Long a2) {
		throw new RuntimeException("String used in minor than equals (<=) need to be a number.");
	}

	public boolean minorThanEquals(Double a1, String a2) {
		throw new RuntimeException("String used in minor than equals (<=) need to be a number.");
	}

	public boolean minorThanEquals(String a1, Double a2) {
		throw new RuntimeException("String used in minor than equals (<=) need to be a number.");
	}
	
	// greaterThan
	
	public boolean greaterThan(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) > 0;
	}

	public boolean greaterThan(Long a1, Long a2) {
		return a1 > a2;
	}
	
	public boolean greaterThan(Double a1, Double a2) {
		return a1 > a2;
	}
	
	public boolean greaterThan(Long a1, Double a2) {
		return a1 > a2;
	}

	public boolean greaterThan(Double a1, Long a2) {
		return a1 > a2;
	}

	public boolean greaterThan(Long a1, String a2) {
		throw new RuntimeException("String used in greater than (>) need to be a number.");
	}
	
	public boolean greaterThan(String a1, Long a2) {
		throw new RuntimeException("String used in greater than (>) need to be a number.");
	}

	public boolean greaterThan(Double a1, String a2) {
		throw new RuntimeException("String used in greater than (>) need to be a number.");
	}

	public boolean greaterThan(String a1, Double a2) {
		throw new RuntimeException("String used in greater than (>) need to be a number.");
	}
	
	// greaterThanEquals
	
	public boolean greaterThanEquals(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) >= 0;
	}

	public boolean greaterThanEquals(Long a1, Long a2) {
		return a1 >= a2;
	}
	
	public boolean greaterThanEquals(Double a1, Double a2) {
		return a1 >= a2;
	}
	
	public boolean greaterThanEquals(Long a1, Double a2) {
		return a1 >= a2;
	}

	public boolean greaterThanEquals(Double a1, Long a2) {
		return a1 >= a2;
	}

	public boolean greaterThanEquals(Long a1, String a2) {
		throw new RuntimeException("String used in greater than equals (>=) need to be a number.");
	}
	
	public boolean greaterThanEquals(String a1, Long a2) {
		throw new RuntimeException("String used in greater than equals (>=) need to be a number.");
	}

	public boolean greaterThanEquals(Double a1, String a2) {
		throw new RuntimeException("String used in greater than equals (>=) need to be a number.");
	}

	public boolean greaterThanEquals(String a1, Double a2) {
		throw new RuntimeException("String used in greater than equals (>=) need to be a number.");
	}
	
	// equalsTo
	
	public boolean equalsTo(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) == 0;
	}

	public boolean equalsTo(Long a1, Long a2) {
		return a1.compareTo(a2) == 0;
	}
	
	public boolean equalsTo(Double a1, Double a2) {
		return a1.compareTo(a2) == 0;
	}
	
	public boolean equalsTo(Long a1, Double a2) {
		return new Double(a1).compareTo(a2) == 0;
	}

	public boolean equalsTo(Double a1, Long a2) {
		return a1.compareTo(new Double(a2)) == 0;
	}

	public boolean equalsTo(Long a1, String a2) {
		throw new RuntimeException("String used in equals (==) need to be a number.");
	}
	
	public boolean equalsTo(String a1, Long a2) {
		throw new RuntimeException("String used in equals (==) need to be a number.");
	}

	public boolean equalsTo(Double a1, String a2) {
		throw new RuntimeException("String used in equals (==) need to be a number.");
	}

	public boolean equalsTo(String a1, Double a2) {
		throw new RuntimeException("String used in equals (==) need to be a number.");
	}
	
	// notEqualsTo
	
	public boolean notEqualsTo(String a1, String a2) {
		logger.warn("String comparision can be different in different databases. Use with caution (or do not use it at all).");
		return a1.compareTo(a2) != 0;
	}

	public boolean notEqualsTo(Long a1, Long a2) {
		return a1.compareTo(a2) != 0;
	}
	
	public boolean notEqualsTo(Double a1, Double a2) {
		return a1.compareTo(a2) != 0;
	}
	
	public boolean notEqualsTo(Long a1, Double a2) {
		return new Double(a1).compareTo(a2) != 0;
	}

	public boolean notEqualsTo(Double a1, Long a2) {
		return a1.compareTo(new Double(a2)) != 0;
	}

	public boolean notEqualsTo(Long a1, String a2) {
		throw new RuntimeException("String used in not equals (!=) need to be a number.");
	}
	
	public boolean notEqualsTo(String a1, Long a2) {
		throw new RuntimeException("String used in not equals (!=) need to be a number.");
	}

	public boolean notEqualsTo(Double a1, String a2) {
		throw new RuntimeException("String used in not equals (!=) need to be a number.");
	}

	public boolean notEqualsTo(String a1, Double a2) {
		throw new RuntimeException("String used in not equals (!=) need to be a number.");
	}
}
