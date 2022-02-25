package com.gridgain.ignite.ggnode.feeder;

import com.gridgain.ignite.ggnode.model.entities.WeekdayDuration;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.time.*;
import java.time.temporal.ChronoUnit;

public class TTLFixedExpiryFeeder {

    private static int ttlDays = 7;     // Note: Add 1 ttlDay if ttlHours (below) is negative.
    private static int ttlHours = 22;   // Note: Can also be specified as 8 ttlDays and -2 ttlHours


    public static int getNumDaysUntilSunday(LocalDateTime dt) {
        return dt.getDayOfWeek() == DayOfWeek.SUNDAY ? 0 : 7 - dt.getDayOfWeek().getValue();
    }

    public static Duration getDurationRoundedTo(LocalDateTime dt, int numDays, int numHours) {
        LocalDateTime endExpiry = dt.plusDays(numHours <= 0 ? numDays : numDays + 1).toLocalDate().atStartOfDay();
        LocalDateTime begExpiry = endExpiry.minusHours(numHours <= 0 ? -numHours : 24 - numHours);

        Duration d = new Duration(dt.toInstant(ZoneOffset.UTC).toEpochMilli(), begExpiry.toInstant(ZoneOffset.UTC).toEpochMilli());
        // System.out.println("DT: " + dt + ", duration: " + d.getDurationAmount() + ", Begin Expiry: " + begExpiry + ", End Expiry: " + endExpiry);
        return d;
    }

    public static void main(String[] args) throws InterruptedException {

        long ttl = 10000L;
        // ExpiryPolicy plc = new ModifiedExpiryPolicy(new Duration(MILLISECONDS, ttl));
        // ExpiryPolicy plc = new ModifiedExpiryPolicy(new WeekdayDuration());

        for (int i=0; i<10; i++) {

            LocalDateTime loadTime = LocalDateTime.now();
            Duration expiryDuration = getDurationRoundedTo(loadTime, ttlDays + getNumDaysUntilSunday(loadTime), ttlHours);
            long expiryDurationInSecs = expiryDuration.getDurationAmount() / 1000L;

            LocalDateTime expiryTime = loadTime.plus(expiryDurationInSecs, ChronoUnit.SECONDS);
            System.out.println("loadTime: " + loadTime + ", expiryTime: " + expiryTime + ", expiryDurationInSecs: " + expiryDurationInSecs);
            Thread.sleep(1000);

        }
    }
}




