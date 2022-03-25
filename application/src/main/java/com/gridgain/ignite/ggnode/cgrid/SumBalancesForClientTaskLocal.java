package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SumBalancesForClientTaskLocal extends ComputeTaskAdapter<Integer, BigDecimal> {

    /** {@inheritDoc} */
    @Nullable
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer clientId) {
        return Collections.singletonMap(new SumBalancesForClientTaskJob(clientId), subgrid.get(0));
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public BigDecimal reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class SumBalancesForClientTaskJob extends ComputeJobAdapter {
        /** */
        private static final Log log = LogFactory.getLog(SumBalancesForClientTaskBinary.class);

        @IgniteInstanceResource
        Ignite _ignite;
        int clientId;

        private SumBalancesForClientTaskJob(int clientId) {
            this.clientId = clientId;
        }

        /** {@inheritDoc} */
        @Override public BigDecimal execute() {
            log.debug(String.format("SumBalancesForClientTaskLocal: initiated for client id: %d", clientId));

            BigDecimal sum = null;
            var accountCfg = Account.getCacheConfiguration();

            try (var accountCache = _ignite.getOrCreateCache(accountCfg))
            {
                IgniteCache<BinaryObject, BinaryObject> accountCacheKB = accountCache.withKeepBinary();

                // Set the SQL query to sum the balances of all accounts for the client with give clientId.
                // Run this query only on the local node that is executing this task, i.e. setLocal(true).
                SqlFieldsQuery sql = new SqlFieldsQuery("SELECT SUM(balance) FROM Account WHERE clientId = ?")
                        .setArgs(clientId)
                        .setLocal(true);

                // Execute SQL query and return the aggregated sum balance.
                List<List<?>> result = accountCacheKB.query(sql).getAll();
                sum = (BigDecimal)result.get(0).get(0);

                log.debug(String.format("SumBalancesForClientTask: Client (id=%d) aggregate balance: %,.2f", clientId, sum));
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            return sum;
        }
    }
}
