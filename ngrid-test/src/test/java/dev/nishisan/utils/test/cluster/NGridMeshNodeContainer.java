package dev.nishisan.utils.test.cluster;

import com.github.dockerjava.api.model.Capability;
import org.testcontainers.containers.Network;

/**
 * Variante de {@link NGridNodeContainer} configurada em <b>full-mesh</b>: cada
 * nó conhece os outros dois peers desde o boot (via {@code SEED_HOST} +
 * {@code NG_PEER_2}), em vez da topologia de seed único usada pelos demais ITs.
 *
 * <p>
 * Reproduz o cenário da issue #117 — peers iniciados concorrentemente que
 * sofrem o <i>simultaneous open</i> TCP. Opcionalmente aplica latência de rede
 * via netem ({@code NETEM_DELAY}, requer {@code NET_ADMIN}) para alargar a
 * janela da colisão em rede bridge.
 */
public class NGridMeshNodeContainer extends NGridNodeContainer {

    /**
     * @param nodeId    identificador/alias DNS do nó
     * @param peerA     primeiro peer (host:porta), ex.: {@code "node-b:9000"}
     * @param peerB     segundo peer (host:porta), ex.: {@code "node-c:9000"}
     * @param netemDelay latência netem a aplicar (ex.: {@code "80ms"}); {@code null}/vazio desativa
     * @param network   rede Docker compartilhada
     */
    public NGridMeshNodeContainer(String nodeId, String peerA, String peerB, String netemDelay, Network network) {
        super(nodeId, peerA, network); // SEED_HOST = peerA
        withEnv("NG_PEER_2", peerB);   // segundo peer da malha
        if (netemDelay != null && !netemDelay.isBlank()) {
            withEnv("NETEM_DELAY", netemDelay);
            withCreateContainerCmdModifier(cmd -> {
                var hostConfig = cmd.getHostConfig();
                if (hostConfig != null) {
                    hostConfig.withCapAdd(Capability.NET_ADMIN);
                }
            });
        }
    }
}
