package shards;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

public class BatchExecutor {

    private List<Set<String>> shardsSelectedForQueries = Lists.newArrayList();

    public void addQuery(Set<String> shardsUsedInQuery) {
        shardsSelectedForQueries.add(shardsUsedInQuery);
    }

    public int[] executeBatch(Map<String, Statement> statements) throws SQLException {
        List<int[]> results = executeStatements(statements);
        int[] updatedCount = mergeResults(results);
        return updatedCount;
    }

    private List<int[]> executeStatements(Map<String, Statement> statements) throws SQLException {
        List<int[]> results = Lists.newArrayList();
        for (String shard : statements.keySet()) {
            Statement statement = statements.get(shard);
            int[] result = statement.executeBatch();
            int[] normalizedResult = normalizeResult(shard, result);
            results.add(normalizedResult);
        }
        return results;
    }

    /** wstaw 0 w miejscach gdzie zapytanie zostalo pominiete dla sharda */
    private int[] normalizeResult(String shard, int[] result) {
        int[] normalizedResult = new int[shardsSelectedForQueries.size()];
        int i = 0;
        for (int t = 0; t < shardsSelectedForQueries.size(); t++) {
            Set<String> shards = shardsSelectedForQueries.get(t);
            if (shards.contains(shard)) {
                normalizedResult[t] = result[i++];
            } else {
                normalizedResult[t] = 0;
            }
        }
        return normalizedResult;
    }

    private int[] mergeResults(List<int[]> results) {
        int[] updatedCount = new int[shardsSelectedForQueries.size()];
        for (int[] result : results) {
            for (int i = 0; i < result.length; i++) {
                updatedCount[i] += result[i];
            }
        }
        return updatedCount;
    }

}
