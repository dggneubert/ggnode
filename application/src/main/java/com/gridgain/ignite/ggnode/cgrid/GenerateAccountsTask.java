package com.gridgain.ignite.ggnode.cgrid;

import com.gridgain.ignite.ggnode.model.entities.Account;
import com.gridgain.ignite.ggnode.model.entities.AccountKey;
import com.gridgain.ignite.ggnode.model.entities.Client;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@ComputeTaskNoResultCache
public class GenerateAccountsTask implements IgniteRunnable
{
    @IgniteInstanceResource
    private Ignite _ignite;

    private final long _clientFirstKey;
    private final long _clientKeyCount;
    private final String _accountCacheName;
    private final long _numAccountsPerClient;

    public static List<IgniteRunnable> BuildTaskBatches(long numClients, String cacheName, long numAccountsPerClient, long batchCount, long batchSize)
    {
        var tasks = new ArrayList<IgniteRunnable>();
        var lastBatch = batchCount - 1;

        for (long batch = 0; batch < batchCount; batch++)
        {
            var firstKey = batch * batchSize;
            var lastKey = firstKey + batchSize;

            if (lastKey > numClients || batch == lastBatch)
                lastKey = numClients;

            if (lastKey <= firstKey)
                break;

            tasks.add(new GenerateAccountsTask(firstKey, lastKey - firstKey, cacheName, numAccountsPerClient));
        }

        return tasks;
    }

    public static void TestUsing(IgniteCache<AccountKey, Account> cache, long modulus)
    {
        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT Count(*) FROM Account")).getAll();

        final long[] i = {0};
        final long numAccounts = (long)res.get(0).get(0);

        System.out.println("SELECT Count(*) FROM Account --> " + numAccounts);

        try (var qryCursor = cache.query(new ScanQuery<AccountKey, Account>()))
        {
            qryCursor.forEach(entry -> { long n = ++i[0];
                if (n == 1 || n % modulus == 0 || n == numAccounts)
                    System.out.println(n + ": Account = " + entry.getKey().getId() + ", Client = " + entry.getKey().getClientId() + ", Value = " + entry.getValue());
            });
        }
    }

    public GenerateAccountsTask(long clientFirstKey, long clientKeyCount, String accountCacheName, long numAccountsPerClient)
    {
        _clientFirstKey = clientFirstKey;
        _clientKeyCount = clientKeyCount;
        _accountCacheName = accountCacheName;
        _numAccountsPerClient = numAccountsPerClient;
    }

    public void run()
    {
        Random random = new Random();

        try (IgniteDataStreamer<AccountKey, Account> streamer = _ignite.dataStreamer(_accountCacheName))
        {
            long maxClientId = _clientFirstKey + _clientKeyCount;
            for (long clientId = _clientFirstKey; clientId < maxClientId; clientId++) {
                for (long accountNum = 0; accountNum < _numAccountsPerClient; accountNum++) {
                    var accountId = clientId * _numAccountsPerClient + accountNum;
                    var accountName = String.format("C%d.A%d", clientId, accountNum);
                    var accountBal = (long) random.nextInt(50000);

                    var accountKey = new AccountKey(accountId, clientId);
                    var account = new Account(accountName, 0, accountBal, "New");

                    streamer.addData(accountKey, account);
                }
            }
        }
    }
}
