package com.github.tiger.kafka.utils;

import java.util.concurrent.TimeUnit;

/**
 * @author liuhongming
 */
public class SystemTimeUtil {

    public interface Time {

        Time SYSTEM = new SystemTime();

        /**
         * Returns the current time in milliseconds.
         */
        long milliseconds();

        /**
         * Returns the value returned by `nanoseconds` converted into milliseconds.
         */
        long hiResClockMs();

        /**
         * Returns the current value of the running JVM's high-resolution time source, in nanoseconds.
         *
         * <p>This method can only be used to measure elapsed time and is
         * not related to any other notion of system or wall-clock time.
         * The value returned represents nanoseconds since some fixed but
         * arbitrary <i>origin</i> time (perhaps in the future, so values
         * may be negative).  The same origin is used by all invocations of
         * this method in an instance of a Java virtual machine; other
         * virtual machine instances are likely to use a different origin.
         */
        long nanoseconds();

        /**
         * Sleep for the given number of milliseconds
         */
        void sleep(long ms);

    }


    public static class SystemTime implements Time {

        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        @Override
        public long hiResClockMs() {
            return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
        }

        @Override
        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                // just wake up early
                Thread.currentThread().interrupt();
            }
        }

    }

}
