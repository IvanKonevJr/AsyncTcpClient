package com.sevenparadigms.tcpclient;

import com.sevenparadigms.dsl.R2dbcDslRepository;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * TcpClient initialization demonstration
 */
public class TcpClientDemo extends AsyncTcpClient {

    /**
     * Init of TcpClient
     *
     * @param hosts
     * @param dslRepository
     */
    public TcpClientDemo(String hosts, R2dbcDslRepository dslRepository) {
        super(hosts, dslRepository);
    }

    @NotNull
    @Override
    public Mono<List<Object>> getNext() {
        return null;
    }

    @Override
    public void sendNext(@NotNull Object entity) {

    }

    @Override
    public void receive(@NotNull byte[] bytes) {

    }
}
