package shards;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

import shards.QueryVisitor.ParameterInfoBean;

import com.google.common.collect.ImmutableSet;

public class GlobalShardsSelectionStrategyTest {
	@Test
	public void mirroringStrategy() {
		GlobalShardsSelectionStrategy globalShardsSelectionStrategy = new GlobalShardsSelectionStrategy();
		globalShardsSelectionStrategy.setConfiguration(JUnitHelper.mockConfiguration());
		
		Set<String> allShards = globalShardsSelectionStrategy.selectShards(new ParameterInfoBean());
		assertEquals(ImmutableSet.of("shard1", "shard2"), allShards);
		
		ParameterInfoBean info = new ParameterInfoBean();
		info.setTable("NotExisting");
		allShards = globalShardsSelectionStrategy.selectShards(info);
		assertEquals(ImmutableSet.of("shard1", "shard2"), allShards);
		
		allShards = globalShardsSelectionStrategy.selectShards(mockParamInfo(Operation.UPDATE));
		assertEquals(ImmutableSet.of("shard1", "shard2"), allShards);
		
		allShards = globalShardsSelectionStrategy.selectShards(mockParamInfo(Operation.SELECT));
		assertEquals(1, allShards.size());
		assertTrue(allShards.contains("shard1") || allShards.contains("shard2"));
	}
	
	private ParameterInfo mockParamInfo(Operation op) {
		return new QueryVisitor.ParameterInfoBean("column", "table", op);
	}
}
