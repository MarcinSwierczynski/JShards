package utils;

import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class SetsUtils {

	/**
	 * Creates a new set containing elements from both sets (OR)
	 */
	public static <T> Set<T> sum(Set<T> set1, Set<T> set2) {
		HashSet<T> result = Sets.newHashSet(set1);
		result.addAll(set2);
		return result;
	}
	
	public static <T> Set<T> sum(List<Set<T>> list) {
		if (list == null || list.size() == 0) {
			return ImmutableSet.of();
		}
		Set<T> results = list.get(0);
		for (int i = 1; i < list.size(); i++) {
			results = sum(results, list.get(i));
		}
		return results;
	}

	/**
	 * Creates a new set containing interception of specified sets (AND)
	 */
	public static <T> Set<T> interception(Set<T> set1, Set<T> set2) {
		Set<T> sum = sum(set1,set2);
		Set<T> difference1 = Sets.newHashSet(set1);
		difference1.removeAll(set2);
		Set<T> difference2 = Sets.newHashSet(set2);
		difference2.removeAll(set1);
		Set<T> differenceSum = sum(difference1, difference2);
		sum.removeAll(differenceSum);
		return sum;
	}
	
	public static <T> Set<T> interception(List<Set<T>> list) {
		if (list == null || list.size() == 0) {
			return ImmutableSet.of();
		}
		Set<T> results = list.get(0);
		for (int i = 1; i < list.size(); i++) {
			results = interception(results, list.get(i));
		}
		return results;
	}

}
