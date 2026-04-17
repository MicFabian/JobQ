package com.jobq;

import org.springframework.util.ClassUtils;

/**
 * Utility for resolving implicit JobQ job type names.
 */
public final class JobTypeNames {

    private JobTypeNames() {}

    public static String defaultTypeFor(Class<?> ownerClass) {
        Class<?> targetClass = ClassUtils.getUserClass(ownerClass);
        String simpleName = targetClass.getSimpleName();
        if (simpleName == null || simpleName.isBlank()) {
            throw new IllegalStateException("Unable to derive implicit JobQ type name for " + targetClass.getName()
                    + ". Declare @Job(\"...\") explicitly.");
        }
        return simpleName;
    }

    public static String configuredOrDefault(String configuredType, Class<?> ownerClass) {
        String normalized = configuredType == null ? "" : configuredType.trim();
        return normalized.isEmpty() ? defaultTypeFor(ownerClass) : normalized;
    }
}
