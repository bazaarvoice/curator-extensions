package com.bazaarvoice.curator.dropwizard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.netflix.curator.ensemble.EnsembleProvider;
import org.apache.zookeeper.client.ConnectStringParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.SortedSet;

/**
 * An ensemble provider that performs DNS resolution to build the connection string. This strategy allows the connection
 * string to adapt to changes in DNS. All resolvable hosts in the original connection string are expanded to their IP
 * addresses. Hosts with multiple IP addresses (i.e., those with multiple A records) will result in multiple
 * corresponding servers in the expanded connection string. The servers in the expanded connection string are sorted to
 * produce a canonical form.
 * <p>
 * NOTE: There are two main things that could cause results that may differ from expectations.
 * </p>
 * <p>
 * Firstly, Java performs its own DNS caching. Cache TTLs can be configured with security properties
 * {@code networkaddress.cache.ttl} and {@code networkaddress.cache.negative.ttl} or corresponding system properties
 * {@code sun.net.inetaddr.ttl} and {@code sun.net.inetaddr.negative.ttl}. The default cache TTL is implementation
 * specific, but typically 30 seconds if there is no security manager installed and infinite if there is. The default
 * negative cache (caching nonexistence of a record) TTL is 10 seconds.
 * </p>
 * <p>
 * Secondly, for a consistent connection string, the DNS provider must produce all records for a hostname. Some DNS
 * servers will only provide a subset of the records for a given hostname - for example, tinydns returns a maximum of 8
 * records in response to a query.
 * </p>
 */
public class ResolvingEnsembleProvider implements EnsembleProvider {
    private final ConnectStringParser _connectStringParser;
    private final Resolver _resolver;

    /**
     * @param connectString The original connections string.
     */
    public ResolvingEnsembleProvider(String connectString) {
        this(connectString, new Resolver());
    }

    @VisibleForTesting
    ResolvingEnsembleProvider(String connectString, Resolver resolver) {
        _resolver = resolver;
        _connectStringParser = new ConnectStringParser(connectString);
    }

    /**
     * Does nothing, as no initialization is required for this provider.
     * @throws Exception Never.
     */
    @Override
    public void start() throws Exception {
        // Do nothing.
    }

    @Override
    public String getConnectionString() {
        StringBuilder connectStringBuilder = new StringBuilder();
        SortedSet<String> addresses = Sets.newTreeSet();

        for (InetSocketAddress hostAndPort : _connectStringParser.getServerAddresses()) {
            try {
                for (InetAddress address : _resolver.lookupAllHostAddr(hostAndPort.getHostName())) {
                    addresses.add(HostAndPort.fromParts(address.getHostAddress(), hostAndPort.getPort()).toString());
                }
            } catch (UnknownHostException e) {
                // Leave unresolvable host in connect string as-is.
                addresses.add(hostAndPort.toString());
            }
        }

        Joiner.on(',').appendTo(connectStringBuilder, addresses);

        if (_connectStringParser.getChrootPath() != null) {
            connectStringBuilder.append(_connectStringParser.getChrootPath());
        }

        return connectStringBuilder.toString();
    }

    /**
     * Does nothing, as no cleanup is required for this provider.
     * @throws IOException Never.
     */
    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    @VisibleForTesting
    static class Resolver {
        public InetAddress[] lookupAllHostAddr(String name) throws UnknownHostException {
            return InetAddress.getAllByName(name);
        }
    }
}
