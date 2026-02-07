/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.metrics;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable representation of an operational alert raised by the
 * {@link NGridAlertEngine}.
 *
 * @param alertType category identifier (e.g. {@code "PERSISTENCE_FAILURE"},
 *                  {@code "HIGH_REPLICATION_LAG"})
 * @param severity  alert severity level
 * @param message   human-readable description of the alert condition
 * @param timestamp when the alert was raised
 * @param metadata  additional key-value context (e.g. node IDs, thresholds)
 * @since 2.1.0
 */
public record NGridAlert(
        String alertType,
        AlertSeverity severity,
        String message,
        Instant timestamp,
        Map<String, String> metadata) {

    /**
     * Convenience factory for creating an alert with current timestamp and no extra
     * metadata.
     */
    public static NGridAlert of(String alertType, AlertSeverity severity, String message) {
        return new NGridAlert(alertType, severity, message, Instant.now(), Map.of());
    }

    /**
     * Convenience factory for creating an alert with current timestamp and
     * metadata.
     */
    public static NGridAlert of(String alertType, AlertSeverity severity, String message,
            Map<String, String> metadata) {
        return new NGridAlert(alertType, severity, message, Instant.now(), metadata);
    }
}
