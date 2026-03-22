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

package dev.nishisan.utils.ngrid.structures;

/**
 * Deployment profile that controls configuration guardrails.
 *
 * <p>When set to {@link #PRODUCTION}, the {@link NGridConfig.Builder#build()}
 * method enforces safety invariants that prevent common production misconfigurations:
 * <ul>
 *   <li>{@code strictConsistency} must be {@code true}</li>
 *   <li>{@code replicationFactor} must be {@code >= 2}</li>
 *   <li>{@code mapPersistenceMode} must not be {@code DISABLED} if maps are configured</li>
 * </ul>
 *
 * <p>In {@link #DEV} and {@link #STAGING} modes, no additional validations
 * are enforced beyond the basic structural checks.
 *
 * @since 3.2.0
 */
public enum DeploymentProfile {

    /**
     * Development profile — no guardrails enforced.
     * Suitable for local development and experimentation.
     */
    DEV,

    /**
     * Staging profile — no guardrails enforced.
     * Suitable for pre-production testing with production-like configs.
     */
    STAGING,

    /**
     * Production profile — strict guardrails enforced.
     * Build will fail if critical safety invariants are violated.
     */
    PRODUCTION
}
