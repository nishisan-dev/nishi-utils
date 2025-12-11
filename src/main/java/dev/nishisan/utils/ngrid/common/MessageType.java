package dev.nishisan.utils.ngrid.common;

/**
 * Enumerates the built-in message types exchanged between transport and higher-level
 * components. Additional application specific commands can be encoded using the
 * {@link ClusterMessage#qualifier()} field.
 */
public enum MessageType {
    HANDSHAKE,
    PEER_UPDATE,
    HEARTBEAT,
    REPLICATION_REQUEST,
    REPLICATION_ACK,
    CLIENT_REQUEST,
    CLIENT_RESPONSE
}
