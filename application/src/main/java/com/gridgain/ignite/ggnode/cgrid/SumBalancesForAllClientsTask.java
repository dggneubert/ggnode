package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class SumBalancesForAllClientsTask extends ComputeTaskAdapter<BigDecimal, Map<Long, BigDecimal>> {

    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, BigDecimal threshold) {
        return subgrid.stream()
            .collect(toMap(node -> new SumBalancesForAllClientsJob(threshold), identity()));
    }

    @Override public Map<Long, BigDecimal> reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<Long, BigDecimal> res = new HashMap<>();
        for (ComputeJobResult result : results) {
            Map<Long, BigDecimal> data = result.getData();

            res.putAll(data);
        }
        return res;
    }

    public static class SumBalancesForAllClientsJob extends ComputeJobAdapter {

        @IgniteInstanceResource
        private Ignite ignite;

        private final BigDecimal threshold;

        public SumBalancesForAllClientsJob(BigDecimal threshold) {
            this.threshold = threshold;
        }

        @Override public Object execute() throws IgniteException {
            IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache(Account.CACHE_NAME).withKeepBinary();
            Map<Long, BigDecimal> res = new HashMap<>();

            for (Cache.Entry<BinaryObject, BinaryObject> e : cache.localEntries()) {
                Long clientId = e.getKey().field("clientId");
                BigDecimal balance = e.getValue().field("balance");
                res.compute(
                    clientId,
                    (k, v) -> {
                        if (v == null)
                            return balance;
                        else
                            return v.add(balance);
                    }
                );
            }

            return res.entrySet().stream()
                .filter(e -> e.getValue().compareTo(threshold) > 0)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }
}