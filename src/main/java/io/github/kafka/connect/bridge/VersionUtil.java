package io.github.kafka.connect.bridge;

/**
 * Shared helper for reporting plugin version in Connector and Task classes.
 */
public final class VersionUtil {

    private static final String FALLBACK_VERSION = "1.0.0";

    private VersionUtil() {
    }

    public static String version(Class<?> type) {
        Package sourcePackage = type.getPackage();
        if (sourcePackage != null && sourcePackage.getImplementationVersion() != null) {
            return sourcePackage.getImplementationVersion();
        }
        return FALLBACK_VERSION;
    }
}
