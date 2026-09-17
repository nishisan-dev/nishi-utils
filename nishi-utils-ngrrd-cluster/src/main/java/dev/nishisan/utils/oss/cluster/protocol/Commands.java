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

package dev.nishisan.utils.oss.cluster.protocol;

import java.util.Set;

/**
 * Constantes de comando do protocolo do cluster ngrrd, usadas como
 * {@code ClientRequestPayload.command()} nas mensagens {@code CLIENT_REQUEST}
 * (padrão idêntico ao usado por {@code DistributedQueue}/{@code DistributedMap}
 * no core). Prefixo {@code ngrrd.} em todos os comandos.
 */
public final class Commands {

    /** Coordenador (líder): cria/idempotentemente retorna o placement de uma série. */
    public static final String PLACE = "ngrrd.place";
    /** Dono: abre (ou confirma aberta) uma série local. */
    public static final String OPEN = "ngrrd.open";
    /** Dono: aplica um lote de amostras. */
    public static final String WRITE_BATCH = "ngrrd.writeBatch";
    /** Dono: força checkpoint de uma série. */
    public static final String CHECKPOINT = "ngrrd.checkpoint";
    /** Dono: força flush de uma série. */
    public static final String FLUSH = "ngrrd.flush";
    /** Dono: lê uma série via {@code ViewQuery}. */
    public static final String READ = "ngrrd.read";
    /** Dono: lê uma série via preset nomeado. */
    public static final String READ_PRESET = "ngrrd.readPreset";
    /** Dono: libera a referência de uma série no registry local. */
    public static final String CLOSE = "ngrrd.close";

    /** Origem: inicia a migração de uma série para outro storage node. */
    public static final String MIGRATE_START = "ngrrd.migrate.start";
    /** Destino: recebe um chunk de bytes da série em migração. */
    public static final String MIGRATE_CHUNK = "ngrrd.migrate.chunk";
    /** Destino: confirma a integridade dos bytes recebidos e ativa a cópia. */
    public static final String MIGRATE_COMMIT = "ngrrd.migrate.commit";
    /** Origem/destino: aborta uma migração em curso. */
    public static final String MIGRATE_ABORT = "ngrrd.migrate.abort";
    /** Origem: apaga a cópia local após a migração ter sido confirmada no destino. */
    public static final String MIGRATE_FINISH = "ngrrd.migrate.finish";
    /** Destino: consulta o status de uma migração. */
    public static final String MIGRATE_STATUS = "ngrrd.migrate.status";

    /** Coordenador (líder): marca um nó como {@code DRAINING}. */
    public static final String ADMIN_DRAIN = "ngrrd.admin.drain";
    /** Coordenador (líder): marca um nó como {@code ACTIVE} novamente. */
    public static final String ADMIN_ACTIVATE = "ngrrd.admin.activate";
    /** Coordenador (líder): retorna o status geral do cluster. */
    public static final String ADMIN_STATUS = "ngrrd.admin.status";
    /** Qualquer nó: retorna as métricas locais do nó. */
    public static final String ADMIN_METRICS = "ngrrd.admin.metrics";
    /** Coordenador (líder): dispara um ciclo de rebalanceamento imediato. */
    public static final String ADMIN_REBALANCE = "ngrrd.admin.rebalance";

    /**
     * Comandos atendidos exclusivamente pelo líder do cluster.
     *
     * <p>{@link #ADMIN_METRICS} fica de fora deliberadamente: métricas são
     * locais a cada nó (ver {@code StorageRequestHandler.metricsSnapshot()}) e
     * qualquer storage node — líder ou não — responde por si mesmo.</p>
     */
    public static final Set<String> LEADER_COMMANDS = Set.of(
            PLACE, ADMIN_DRAIN, ADMIN_ACTIVATE, ADMIN_STATUS, ADMIN_REBALANCE);

    /** Comandos atendidos pelo dono da série. */
    public static final Set<String> OWNER_COMMANDS = Set.of(
            OPEN, WRITE_BATCH, CHECKPOINT, FLUSH, READ, READ_PRESET, CLOSE);

    /** Comandos do protocolo de migração, atendidos pela origem e/ou pelo destino. */
    public static final Set<String> MIGRATION_COMMANDS = Set.of(
            MIGRATE_START, MIGRATE_CHUNK, MIGRATE_COMMIT, MIGRATE_ABORT, MIGRATE_FINISH, MIGRATE_STATUS);

    private Commands() {
    }
}
