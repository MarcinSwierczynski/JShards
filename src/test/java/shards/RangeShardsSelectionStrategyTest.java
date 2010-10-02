package shards;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import shards.QueryVisitor.ParameterInfoBean;

import com.google.common.collect.ImmutableSet;

public class RangeShardsSelectionStrategyTest {

	private RangeShardsSelectionStrategy strategy;

	@Before
	public void init() {
		strategy = new RangeShardsSelectionStrategy();
		strategy.setColumn("id");
		strategy.addRange(0, 10, "shard1");
		strategy.addRange(11, Long.MAX_VALUE, "shard2");
	}

	@Test
	public void allShards() {
		Set<String> allShards = strategy.selectShards(new ParameterInfoBean());
		assertEquals(ImmutableSet.of("shard1", "shard2"), allShards);
	}

	@Test
	public void equal() {
		ParameterInfo info = createParameterInfo("id", Operator.EQUAL, 5);
		Set<String> shards = strategy.selectShards(info);
		assertEquals(shards.size(), 1);
		assertEquals("shard1", shards.iterator().next());
	}
	
	@Test
	public void notEqual() {
		ParameterInfo info = createParameterInfo("id", Operator.NOT_EQUAL, 5);
		Set<String> shards = strategy.selectShards(info);
		assertEquals(shards.size(), 2);
		assertEquals(shards, ImmutableSet.of("shard1", "shard2"));
	}
	
	@Test
	public void less() {
		ParameterInfo info = createParameterInfo("id", Operator.LESS, 11);
		Set<String> shards = strategy.selectShards(info);
		assertEquals(shards.size(), 1);
		assertEquals("shard1", shards.iterator().next());
	}

	private ParameterInfo createParameterInfo(String column, Operator operator, long value) {
		ParameterInfoBean info = new ParameterInfoBean();
		info.setColumn(column);
		info.setOperator(operator);
		info.setValue(value);
		return info;
	}
	
}
