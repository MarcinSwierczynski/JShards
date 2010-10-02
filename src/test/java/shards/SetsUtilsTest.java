package shards;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import utils.SetsUtils;

import com.google.common.collect.ImmutableSet;

public class SetsUtilsTest {

	@Test
	public void interception() {
		ImmutableSet<String> set1 = ImmutableSet.of("shard1","shard2");
		ImmutableSet<String> set2 = ImmutableSet.of("shard1");
		Set<String> set = SetsUtils.interception(set1, set2);
		assertNotNull(set);
		assertEquals(set.size(), 1);
		assertEquals(set.iterator().next(), "shard1");
		
		set1 = ImmutableSet.of("shard1","shard2");
		set2 = ImmutableSet.of();
		set = SetsUtils.interception(set1, set2);
		assertNotNull(set);
		assertEquals(set.size(), 0);
		
		set1 = ImmutableSet.of("shard1","shard2");
		set2 = ImmutableSet.of("shard1","shard2");
		set = SetsUtils.interception(set1, set2);
		assertNotNull(set);
		assertEquals(set.size(), 2);
		Iterator<String> iterator = set.iterator();
		assertEquals(iterator.next(), "shard1");
		assertEquals(iterator.next(), "shard2");
	}

}
