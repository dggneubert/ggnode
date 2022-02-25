package com.gridgain.ignite.ggnode.model.entities;

import javax.cache.expiry.Duration;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * WeekdayDuration is factory-like class that contains only static methods that return
 * javax.cache.expiry.Duration instances with calculated duration times (from the current local time)
 * specified by arguments: DayOfWeek, hour (0-23), and minimum number of weeks the duration must span.
 */
public class WeekdayDuration {

    /**
     * Returns a javax.cache.expiry.Duration with a calculated duration (from the current local time)
     * that will expire on the next Sunday at the specified hour and span at least 1 week.
     * @param untilHour - the hour (0-23) on with the duration should expire
     * @return a javax.cache.expiryDuration with the calculated duration based upon the current local time.
     */
    public static Duration SundayDuration(int untilHour) {
        return WeekdayDuration(DayOfWeek.SUNDAY, untilHour, 1);
    }

    /**
     * Returns a javax.cache.expiry.Duration with a calculated duration (from the current local time)
     * that will expire on the next specified weekday and hour.
     * @param weekday - the DayOfWeek on which the duration should expire
     * @param untilHour - the hour (0-23) on with the duration should expire
     * @return a javax.cache.expiryDuration with the calculated duration based upon the current local time.
     */
    public static Duration WeekdayDuration(DayOfWeek weekday, int untilHour) {
        return WeekdayDuration(weekday, untilHour, 0);
    }

    /**
     * Returns a javax.cache.expiry.Duration with a calculated duration (from the current local time) that
     * will expire on the specified weekday and hour, and span the specified minimum number of full weeks.
     * @param weekday - the DayOfWeek on which the duration should expire
     * @param untilHour - the hour (0-23) on with the duration should expire
     * @param minNumOfFullWeeks the minimum number (>= 0) of full weeks the duration must span
     * @return a javax.cache.expiryDuration with the calculated duration based upon the current local time.
     */
    public static Duration WeekdayDuration(DayOfWeek weekday, int untilHour, int minNumOfFullWeeks) {

        if (minNumOfFullWeeks < 0) minNumOfFullWeeks = 0;

        LocalDateTime ldt = LocalDateTime.now();
        int nowDayVal = ldt.getDayOfWeek().getValue();
        int expireDayVal = weekday.getValue();

        int ttlDays = expireDayVal - nowDayVal;                             // to consider the hour before adding a week extension,
        if (ttlDays == 0 && minNumOfFullWeeks == 0) minNumOfFullWeeks = 1;  // optionally, add: && ldt.getHour() >= untilHour
        if (ttlDays < 0) ttlDays += 7;
        ttlDays += minNumOfFullWeeks * 7;

        LocalDateTime expireldt = ldt.plusDays(ttlDays).toLocalDate().atStartOfDay().plusHours(untilHour);
        return new Duration(ldt.toInstant(ZoneOffset.UTC).toEpochMilli(), expireldt.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

}

