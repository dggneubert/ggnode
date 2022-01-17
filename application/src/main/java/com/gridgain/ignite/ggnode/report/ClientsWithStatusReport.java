package com.gridgain.ignite.ggnode.report;

import com.gridgain.ignite.ggnode.dao.ClientDao;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;

import java.util.List;

/**
 * Created By GridGain Systems
 *
 * Class serves as a reporting client for Inventory items.
 */
public class ClientsWithStatusReport {

    private static final Log log = LogFactory.getLog(ClientsWithStatusReport.class);

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite start = Ignition.start("client-config.xml")) {
            // Simple Query
            runClientsWithStatusReportForStatus("Platinum");
        }
    }

    public static void runClientsWithStatusReportForStatus(String status) {
        log.info("**************************************************  REPORT HEADER ***************************************************************************************");
        log.info("*** Started report: ClientsWithStatusReport for status: " + status);
        try (ClientDao clientDao = new ClientDao()) {

            List<Client> clients = clientDao.getClientsWithStatus(status);
            if (clients!=null && !clients.isEmpty()) {
                log.info("*** ClientsWithStatusReport found " + clients.size() + " clients with status: " + status);
                for (Client client : clients) {
                    log.info("*** " + client);
                }
                // log.info(new Gson().toJson(clients);
            } else {
                log.info("*** ClientsWithStatusReport did not find any clients with status: " + status);
            }

        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }
        log.info("**************************************************  REPORT FOOTER ***************************************************************************************");
    }

}

