package com.gridgain.ignite.ggnode.feeder;

import com.gridgain.ignite.ggnode.dao.AccountDao;
import com.gridgain.ignite.ggnode.dao.ClientDao;
import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.AccountKey;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

import static java.lang.Integer.parseInt;

/**
 * This class adds and co-locates client and their associated account records into Client and Account caches.
 * The current affinity co-location implementation adds one client at a time followed immediately with the
 * of (N) client account records with an affinity key on the ClientId field (in the Account Cache).
 */
public class ClientAndAccountsFeeder {

    private static final Log log = LogFactory.getLog(ClientAndAccountsFeeder.class);
    private static final int minAccountType = 1;    // Models different account types (not used in demo queries)
    private static final int numAccountTypes = 10;  // Models different account types (not used in demo queries)
    private static final int maxAccountsPerClient = 10000;

    private static int genAccountIdFor(int clientNum, int accountNum) {
        return clientNum * maxAccountsPerClient + accountNum;
    }

    private static String genClientNameFor(int clientNum) {
        return String.format("C%07d", clientNum);
    }

    private static String genAccountNameFor(int clientNum, int accountNum) {
        return String.format("C%d.A%d", clientNum, accountNum);
    }

    /**
     *  TODO Modify this method to set desired distribution of client aggregate balances.
     */
    private static long genAccountBalanceFor(int clientNum, int accountNum, Random r) {
        return r.nextInt(20 + r.nextInt((clientNum % 100) + (clientNum == 1 ? 1 : 3)));
    }

    private static int genRandomAccountTypeUsing(Random r) {
        return minAccountType + r.nextInt(numAccountTypes);
    }

    private static long genRandomAccountBalanceUsing(int clientNum, int accountNum, Random r, int range) {
        return clientNum * maxAccountsPerClient + accountNum + r.nextInt(range);
    }

    public static void  loadSDemoAccounts(int numClients, int numAccountsPerClient) {

        String accountName; long accountId; int accountType; long accountBalance;

        log.info(String.format("*** SDemo Feeder: Begin feed (%d clients at %d accounts per client).", numClients, numAccountsPerClient));

        try (ClientDao clientDao = new ClientDao(); AccountDao accountDao = new AccountDao()) {

            int numCachedAccounts = 0;
            for (int clientNum = 1; clientNum <= numClients; clientNum++) {

                Client client = new Client(genClientNameFor(clientNum));
                clientDao.save(clientNum, client);

                // TreeMap<AffinityKey<Integer>, Account> clientAccounts = new TreeMap<>();

                Random r = new Random();

                for (int accountNum = 0; accountNum < numAccountsPerClient; accountNum++) {

                    accountId = genAccountIdFor(clientNum, accountNum);
                    accountType = genRandomAccountTypeUsing(r);
                    accountName = genAccountNameFor(clientNum, accountNum);
                    accountBalance = genAccountBalanceFor(clientNum, accountNum, r);
                    // accountBalance = genRandomAccountBalanceUsing(clientNum, accountNum, r, numAccountsPerClient);

                    AccountKey accountKey = new AccountKey(accountId, (long)clientNum);
                    Account account = new Account(accountName, accountType, accountBalance, "New");
                    accountDao.save(accountKey, account);

                    // clientAccounts.put(new AffinityKey<Integer>(account.getId(), account.getClientId()), account);
                }

                numCachedAccounts += numAccountsPerClient;
                // accountDao.saveAll(clientAccounts);

                if (clientNum % 10 == 0) {
                    log.info(String.format("*** SDemo Feeder: Added %d clients (at %d accounts per client).", clientNum, numAccountsPerClient));
                }
            }

            log.info(String.format("*** SDemo Feeder: Added a total of %d clients (at %d accounts per client).", numClients, numAccountsPerClient));
            log.info("*** SDemo Feeder: End feed.");
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }

    }

    public static void  loadNamedClientAccounts() {

        int numStoredAccounts = 0;

        try (AccountDao accountDao = new AccountDao()) {
            log.info("*** Account Feeder Started");

            List<String> accountSpecs = createAccountSpecs();

            for (String args : accountSpecs) {

                // TODO improvie this later (if time permits)
                String[] a = args.split("\\|");
                int clientId = parseInt(a[0]);
                String clientAbrv = a[1];
                int accountId = parseInt(a[2]);
                int numAccounts = parseInt(a[3]);
                int minAccountType = parseInt(a[4]);
                int numAccountTypes = parseInt(a[5]) - minAccountType + 1;
                int minBalance = parseInt(a[6]);
                int maxBalanceDelta = parseInt(a[7]) - minBalance + 1;

                Random r = new Random();
                for (int i=0; i <= numAccounts; i++) {

                    String name = clientAbrv + '-' + Integer.toString(i);
                    int type =  minAccountType + r.nextInt(numAccountTypes);
                    Long balance = Long.valueOf(minBalance + r.nextInt(maxBalanceDelta));

                    AccountKey accountKey = new AccountKey((long)accountId, (long)clientId);
                    Account account = new Account(name, type, balance, "New");
                    accountDao.save(accountKey, account);

                    log.info("*** " + account);
                    accountId++;
                }

                numStoredAccounts += numAccounts;
            }

            log.info("*** " + numStoredAccounts + " Accounts stored in the grid");

            log.info("*** Account Feeder Completed");
        } catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }

    }

    private static List<String> createAccountSpecs() {
        List<String> specs = new ArrayList<String>();
        specs.add("1|acme|10000|10|1|10|0|100");
        specs.add("2|glbx|11000|10|1|10|0|1000");
        specs.add("3|mstr|12000|10|1|10|0|10000");
        specs.add("4|cybd|13000|10|1|10|0|100000");
        specs.add("5|soly|14000|10|1|10|-100000|100000");
        specs.add("6|init|15000|10|1|10|-100000|1000");
        specs.add("7|wonk|16000|10|1|10|0|1000000");
        specs.add("8|gwzb|17000|10|1|10|0|1000000");
        specs.add("9|virt|18000|10|1|10|-10000|10000");
        specs.add("10|cprk|19000|10|1|10|0|100000");
        // specs.add("11|xlc|20000|10000|1|10|0|1000");
        return specs;
    }

}


