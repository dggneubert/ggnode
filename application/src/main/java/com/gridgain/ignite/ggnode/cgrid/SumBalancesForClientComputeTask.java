package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
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

public class SumBalancesForClientComputeTask extends ComputeTaskAdapter<Integer, BigDecimal> {

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
        Ignite ignite;
        int clientId;

        private SumBalancesForClientTaskJob(int clientId) {
            this.clientId = clientId;
        }

        /** {@inheritDoc} */
        @Override public BigDecimal execute() {
            log.debug(String.format("SumBalancesForClientTaskJob -> initiated for client id: %d", clientId));

            // Set SQL query to return the 'balance' field from all rows having the specified clientId
            SqlFieldsQuery qry = new SqlFieldsQuery("SELECT balance FROM ACCOUNT WHERE clientId = ?")
                    .setArgs(clientId)
                    .setLocal(true);

            long sum = 0L;
            boolean isSumValid = false;
            try {
                IgniteCache<Integer, Account> accountCache = ignite.cache("ACCOUNT_CACHE");
                IgniteCache<BinaryObject, BinaryObject> accountCacheKB = accountCache.withKeepBinary();

                QueryCursor<List<?>> accounts = accountCacheKB.query(qry);

                for (List<?> row : accounts.getAll()) {
                    sum += (Long) row.get(0);
                    isSumValid = true;
                }

                // accountCacheKB.close();  TODO should KeepBinary copy be closed?
                accountCache.close();

            } catch (Exception ex) {
                ex.printStackTrace();
                isSumValid = false;
            }

            log.debug("SumBalancesForClientTaskBinary -> Completed ");
            return isSumValid ? new BigDecimal(sum) : BigDecimal.ZERO;   // TODO fix this later, should be NULL or N/A???
        }
    }
}
