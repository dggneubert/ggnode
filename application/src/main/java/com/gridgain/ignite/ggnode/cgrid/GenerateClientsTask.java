package com.gridgain.ignite.ggnode.cgrid;

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

@ComputeTaskNoResultCache
public class GenerateClientsTask implements IgniteRunnable
{
    @IgniteInstanceResource
    private Ignite _ignite;

    private final long _clientFirstKey;
    private final long _clientKeyCount;
    private final String _clientCacheName;

    public static List<IgniteRunnable> BuildTaskBatches(long numClients, String cacheName, long batchCount, long batchSize)
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

            tasks.add(new GenerateClientsTask(firstKey, lastKey - firstKey, cacheName));
        }

        return tasks;
    }

    public static void TestUsing(IgniteCache<Long, Client> cache, int modulus)
    {
        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT Count(*) FROM Client")).getAll();

        final long[] i = {0};
        final long numClients = (long)res.get(0).get(0);

        System.out.println("SELECT Count(*) FROM Client --> " + numClients);

        try (var qryCursor = cache.query(new ScanQuery<Long, Client>()))
        {
            qryCursor.forEach(entry -> { long n = ++i[0];
                if (n == 1 || n % modulus == 0 || n == numClients)
                    System.out.println(n + ": Key = " + entry.getKey() + ", Value = " + entry.getValue());
            });
        }
    }

    public GenerateClientsTask(long clientFirstKey, long clientKeyCount, String clientCacheName)
    {
        _clientFirstKey = clientFirstKey;
        _clientKeyCount = clientKeyCount;
        _clientCacheName = clientCacheName;
    }

    public void run()
    {
        try (IgniteDataStreamer<Long, Client> streamer = _ignite.dataStreamer(_clientCacheName))
        {
            long maxId = _clientFirstKey + _clientKeyCount;
            for (long id = _clientFirstKey; id < maxId; id++)
            {
                var client = new Client(String.format("C%07d", id));
                streamer.addData(id, client);
            }
        }

    }
}
