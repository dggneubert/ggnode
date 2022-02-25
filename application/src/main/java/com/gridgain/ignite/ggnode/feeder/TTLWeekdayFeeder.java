package com.gridgain.ignite.ggnode.feeder;

import com.gridgain.ignite.ggnode.model.entities.WeekdayDuration;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.time.*;
import java.time.temporal.ChronoUnit;

public class TTLWeekdayFeeder {

    public static void main(String[] args) throws InterruptedException {

        int expiryHour = 22;  // set expiration hour to 10 pm
        for (int i=0; i<10; i++) {

            Duration duration = WeekdayDuration.SundayDuration(expiryHour);
            ExpiryPolicy plc = new ModifiedExpiryPolicy(duration);

            // cache.withExpiryPolicy(plc).put(0L, 0L);  TODO <== write the record with the Sunday night expiration

            // Validate/output expiration time to console
            long expiryDurationInSecs = duration.getDurationAmount() / 1000L;
            LocalDateTime loadTime = LocalDateTime.now();
            LocalDateTime expiryTime = loadTime.plus(expiryDurationInSecs, ChronoUnit.SECONDS);
            System.out.println("loadTime: " + loadTime + ", expiryTime: " + expiryTime + ", expiryDurationInSecs: " + expiryDurationInSecs);
            Thread.sleep(1000);
        }
    }
}
