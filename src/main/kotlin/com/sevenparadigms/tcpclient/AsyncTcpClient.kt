package com.sevenparadigms.tcpclient

import com.sevenparadigms.common.debug
import com.sevenparadigms.common.hex
import com.sevenparadigms.common.info
import com.sevenparadigms.common.toJsonNode
import com.sevenparadigms.dsl.R2dbcDslRepository
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelOption
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.NettyInbound
import reactor.netty.NettyOutbound
import reactor.netty.tcp.TcpClient
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

abstract class AsyncTcpClient(var hosts: String,
                              var repository: R2dbcDslRepository<*, *>) {

    private val delaySeconds = 5L

    private val locking = AtomicReference<Boolean>(false)
    private val flux = AtomicReference<FluxSink<ByteBuf?>>()
    private val host = AtomicReference<String>()
    private val it = AtomicReference<Iterator<String>>()
    private val timer = AtomicReference<LocalDateTime>()
    private val trying = AtomicInteger()
    private val lastPayload = AtomicReference<ByteArray>(byteArrayOf())

    init {
        init().subscribe {
            Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate({ this.checkReceiveTimer() }, delaySeconds, delaySeconds, TimeUnit.SECONDS)
            repository.listen()
                    .doOnNext { notification ->
                        val json = notification.parameter!!.toString().toJsonNode()
                        if (json["operation"].asText() == "INSERT") {
                            info("database <= $json")
                            checkAndSend()
                        }
                    }.subscribe()
        }
    }

    private fun init(): Mono<out Connection> {
        locking.set(false)
        flux.set(null)
        host.set(null)
        it.set(hosts.split(",").toSet().iterator())
        timer.set(LocalDateTime.now().plusSeconds(delaySeconds))
        return createClient()
    }

    private fun checkReceiveTimer() {
        if (!locking.get() && LocalDateTime.now().isAfter(timer.get())
                || locking.get() && LocalDateTime.now().isAfter(timer.get().plusSeconds(delaySeconds))) {
            timer.set(LocalDateTime.now().plusSeconds(delaySeconds))
            locking.set(true)
            checkAndSend()
        }
    }

    private fun checkAndSend() {
        if (locking.get()) {
            locking.set(false)
            getNext().subscribe {
                if (it.isNotEmpty()) {
                    sendNext(it.first())
                } else
                    locking.set(true)
            }
        }
    }

    abstract fun getNext(): Mono<List<Any>>
    abstract fun sendNext(entity: Any)
    abstract fun receive(bytes: ByteArray): Mono<Any>

    private fun createClient(): Mono<out Connection> {
        host.set(null)
        if (it.get().hasNext()) {
            val connection = it.get().next()
            val string = connection.split(":").iterator()
            return TcpClient.create().host(string.next()).port(string.next().toInt())
                    .handle { `in`: NettyInbound, out: NettyOutbound ->
                        `in`.receive().asByteArray().doOnNext {
                            debug("received <= [${it.hex()}]")
                            receive(it).subscribe {
                                checkAndSend()
                            }
                        }.subscribe()
                        out.send(Flux.create { sink -> flux.set(sink) })
                    }
                    .option(ChannelOption.AUTO_CLOSE, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .connect()
                    .doOnSuccess {
                        info("Connected to $connection")
                        host.set(connection)
                        locking.set(true)
                        trying.set(0)
                    }
                    .onErrorResume {
                        if (trying.incrementAndGet() > 2) {
                            throw IllegalArgumentException("Host $connection is unavailable")
                        }
                        TimeUnit.MILLISECONDS.sleep(1000)
                        createClient() as Mono<out Nothing>?
                    }
        } else {
            return init()
        }
    }

    @Synchronized
    fun sendMessage(bytes: ByteArray) {
        val counter = AtomicInteger(40)
        while (synchronized(host) { host.get() } == null
                || synchronized(flux) { flux.get() } == null) {
            if (counter.getAndDecrement() == 1) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100)
        }
        if (counter.get() == 0 || lastPayload.get().hex() == bytes.hex()) {
            lastPayload.set(byteArrayOf())
            info("Found lost connection, attempting connect to next host")
            createClient().subscribe {
                forceNext()
            }
        } else {
            debug("sending(${host.get()}) => [${bytes.hex()}]")
            lastPayload.set(bytes)
            timer.set(LocalDateTime.now().plusSeconds(delaySeconds))
            flux.get().next(Unpooled.wrappedBuffer(bytes))
        }
    }

    protected fun forceNext() {
        locking.set(true)
        checkAndSend()
    }
}