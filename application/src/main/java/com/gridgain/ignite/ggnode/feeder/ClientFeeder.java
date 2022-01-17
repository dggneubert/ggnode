package com.gridgain.ignite.ggnode.feeder;

import com.gridgain.ignite.ggnode.dao.ClientDao;

import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map;

/**
 * This class is currently not used, but could be if clients are added in bulk before account records are added.
 * The current affinity co-location implementation in ClientAndAccountsFeeder, adds one client at a time followed
 * immediately with the addition of (N) client account records.
 */
public class ClientFeeder {

    private static final Log log = LogFactory.getLog(ClientFeeder.class);
    private static final ArrayList<String> namedClientList;

    public static void loadSchwabDemoClients(int numClients) {

        log.info(String.format("*** Schwab Demo Client Feeder: Begin client feed  got %d clients.", numClients));

        Random r = new Random();    // For generating random client account status (0..3)

        try(ClientDao clientDao = new ClientDao() ) {

            TreeMap<Integer, Client> clients = new TreeMap<>();
            for (int i=1; i <= numClients; i++) {
                Client client = new Client(i, String.format("C%07d", i), Client.Status[r.nextInt(4)]);
                clients.put(client.getId(), client);

                if (i % 100000 == 0) {
                    log.info(String.format("*** Schwab Demo Client Feeder: Generated %d Client records.", i));
                }
            }

            log.info(String.format("*** Schwab Demo Client Feeder: Saving %d Client records to the Client cache.", numClients));
            clientDao.saveAll(clients);

            int numCachedClients = clientDao.getClientCount();
            log.info(String.format("*** Schwab Demo Client Feeder: Client cache record count is: %d.", numCachedClients));

            if (numCachedClients < numClients) {
                log.error(String.format("*** Schwab Demo Client Feeder: Incomplete Load - only %d of %d clients were cached.", numCachedClients, numClients));
            }

            log.info("*** Schwab Demo Client Feeder: End client feed.");
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }
    }

    public static void loadNamedClients() {

        try(ClientDao clientDao = new ClientDao() ) {
            log.info("*** Start Client Feeder");
            TreeMap<Integer, Client> clients = new TreeMap<>();

            Random r = new Random();
            int clientIdCounter = 1;

            for (String key : namedClientList) {
                Client client = new Client(clientIdCounter, key, Client.Status[r.nextInt(4)]);
                clients.put(client.getId(), client);
                clientIdCounter++;
            }

            clientDao.saveAll(clients);

            log.info("*** " + clients.size() + " clients stored in the grid");

            Map<Integer, Client> storedClients = clientDao.getClients(clients.keySet());

            if (storedClients != null && !storedClients.isEmpty()) {
                for (Client storedC : storedClients.values()) {
                    log.info("*** " + storedC);
                }
            } else {
                log.info("*** No clients were retrieved");
            }

            log.info("*** Client Feeder Completed");
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }
    }

    static {
        namedClientList = new ArrayList<>();
        namedClientList.add("Acme Corporation");
        namedClientList.add("Globex Corporation");
        namedClientList.add("Monsters, Inc.");
        namedClientList.add("Cyberdyne Systems");
        namedClientList.add("Soylent Corp");
        namedClientList.add("Initech");
        namedClientList.add("Wonka Industries");
        namedClientList.add("Gringotts Wizarding Bank");
        namedClientList.add("Virtucon");
        namedClientList.add("Central Perk");
    }

}

