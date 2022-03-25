package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
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

public class SumBalancesForClientTaskAffinity extends ComputeTaskAdapter<Integer, BigDecimal> {

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
            log.debug(String.format("SumBalancesForClientTaskAffinity: initiated for client id: %d", clientId));

            try
            {
                return ignite.compute().affinityCall(Client.CACHE_NAME, clientId, new SumBalancesForClientTask(clientId));
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
            return null;
        }
    }
}
