package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.dao.ClientDao;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;


public class UpdateAllClientStatusTaskExecutor {
    private static final Log log = LogFactory.getLog(UpdateAllClientStatusTaskExecutor.class);

    public static void main(String[] args) {
        log.info("UpdateAllClientStatusTaskExecutor -> Started ");
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("client-config.xml")) {
            IgniteCompute computeRemotes = Ignition.ignite().compute(ignite.cluster().forServers());

            try(ClientDao clientDao = new ClientDao()) {

                clientDao.getAllClientKeys().forEach(clientId -> {
                    Boolean updatedClient = computeRemotes.affinityCall("CLIENT_CACHE", clientId, new UpdateClientStatusTask(clientId));

                    if (updatedClient) {
                        log.info(String.format("Client (id=%d) was updated.", clientId));
                    }

                });

                log.info("UpdateAllClientStatusTaskExecutor -> Completed ");
            } catch (Exception ex) {
                log.error("Exception: " + ex.getMessage(), ex);
            }

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
            ex.printStackTrace();
        }

    }
}
