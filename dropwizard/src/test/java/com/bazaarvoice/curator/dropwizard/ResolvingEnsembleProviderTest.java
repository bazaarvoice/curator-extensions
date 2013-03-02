package com.bazaarvoice.curator.dropwizard;

import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import sun.net.util.IPAddressUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResolvingEnsembleProviderTest {

    private final ResolvingEnsembleProvider.Resolver _resolver = mock(ResolvingEnsembleProvider.Resolver.class);

    @Test
    public void testNameResolves() throws Exception {
        whenQueried("test").thenResolveTo("1.1.1.1");

        ResolvingEnsembleProvider provider = newProvider();

        assertEquals("1.1.1.1:2181", provider.getConnectionString());
    }

    @Test
    public void testNameDoesNotResolve() throws Exception {
        whenQueried("test").thenFail();

        ResolvingEnsembleProvider provider = newProvider();

        assertEquals("test:2181", provider.getConnectionString());
    }

    @Test
    public void testMultipleRecords() throws Exception {
        whenQueried("test").thenResolveTo("1.1.1.1", "2.2.2.2");

        ResolvingEnsembleProvider provider = newProvider();

        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
    }

    @Test
    public void testMultipleRecordsCanonical() throws Exception {
        // Return records in different order to simulate round robin DNS.
        whenQueried("test").thenResolveTo("1.1.1.1", "2.2.2.2").thenResolveTo("2.2.2.2", "1.1.1.1");

        ResolvingEnsembleProvider provider = newProvider();

        // Should provide the same connect string regardless of DNS order.
        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
    }

    @Test
    public void testMultipleNames() throws Exception {
        whenQueried("test1").thenResolveTo("1.1.1.1");
        whenQueried("test2").thenResolveTo("2.2.2.2");

        ResolvingEnsembleProvider provider = newProvider("test1:2181,test2:2181");

        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
    }

    @Test
    public void testChrootPath() throws Exception {
        whenQueried("test").thenResolveTo("1.1.1.1");

        ResolvingEnsembleProvider provider = newProvider("test:2181/chroot");

        assertEquals("1.1.1.1:2181/chroot", provider.getConnectionString());
    }

    @Test
    public void testChrootPathNameDoesNotResolve() throws Exception {
        whenQueried("test").thenFail();

        ResolvingEnsembleProvider provider = newProvider("test:2181/chroot");

        assertEquals("test:2181/chroot", provider.getConnectionString());
    }

    @Test
    public void testChrootPathMultipleRecords() throws Exception {
        whenQueried("test").thenResolveTo("1.1.1.1", "2.2.2.2");

        ResolvingEnsembleProvider provider = newProvider("test:2181/chroot");

        assertEquals("1.1.1.1:2181,2.2.2.2:2181/chroot", provider.getConnectionString());

    }

    private ResolvingEnsembleProvider newProvider() {
        return newProvider("test:2181");
    }

    private ResolvingEnsembleProvider newProvider(String connectString) {
        return new ResolvingEnsembleProvider(connectString, _resolver);
    }

    private ResolverOngoingStubbing whenQueried(String domain) throws Exception {
        return new ResolverOngoingStubbing(domain);
    }

    private class ResolverOngoingStubbing {
        private OngoingStubbing<InetAddress[]> _stub;

        private ResolverOngoingStubbing(String domain) throws Exception {
            _stub = when(_resolver.lookupAllHostAddr(domain));
        }

        private ResolverOngoingStubbing thenResolveTo(String... addresses) throws Exception {
            InetAddress[] result = new InetAddress[addresses.length];

            for (int i = 0; i < addresses.length; ++i) {
                result[i] = InetAddress.getByAddress(IPAddressUtil.textToNumericFormatV4(addresses[i]));
            }

            _stub =_stub.thenReturn(result);

            return this;
        }

        @SuppressWarnings("unchecked")
        private ResolverOngoingStubbing thenFail() {
            _stub = _stub.thenThrow(UnknownHostException.class);

            return this;
        }
    }
}
