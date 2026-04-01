/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.network;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network connector which uses a discovery agent to detect the remote brokers
 * available and setup a connection to each available remote broker
 *
 * @org.apache.xbean.XBean element="networkConnector"
 *
 */
public class DiscoveryNetworkConnector extends NetworkConnector implements DiscoveryListener {
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryNetworkConnector.class);

    private DiscoveryAgent discoveryAgent;
    private Map<String, String> parameters;
    private final ConcurrentMap<URI, DiscoveryEvent> activeEvents = new ConcurrentHashMap<URI, DiscoveryEvent>();
    private URI discoveryUri;
    public DiscoveryNetworkConnector() {
    }

    public DiscoveryNetworkConnector(URI discoveryURI) throws IOException {
        setUri(discoveryURI);
    }

    public void setUri(URI discoveryURI) throws IOException {
        this.discoveryUri = discoveryURI;
        setDiscoveryAgent(DiscoveryAgentFactory.createDiscoveryAgent(discoveryURI));
        try {
            parameters = URISupport.parseParameters(discoveryURI);
            // allow discovery agent to grab it's parameters
            IntrospectionSupport.setProperties(getDiscoveryAgent(), parameters);
        } catch (URISyntaxException e) {
            LOG.warn("{} - failed to parse query parameters from discoveryURI: {}", getName(),discoveryURI, e);
        }
    }

    public URI getUri() {
        return discoveryUri;
    }

    @Override
    public void onServiceAdd(DiscoveryEvent event) {
        // Ignore events once we start stopping.
        if (serviceSupport.isStopped() || serviceSupport.isStopping()) {
            return;
        }
        String url = event.getServiceName();
        if (url != null) {
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                LOG.warn("{} - Could not connect to remote URI: {} due to bad URI syntax: ",getName(), url, e);
                return;
            }

            if (localURI.equals(uri)) {
                LOG.debug("{} - not connecting loopback: {}", getName(),uri);
                return;
            }

            if (connectionFilter != null && !connectionFilter.connectTo(uri)) {
                LOG.debug("{} - connectionFilter disallows connection to: {}", getName(),uri);
                return;
            }

            // Should we try to connect to that URI?
            if (activeEvents.putIfAbsent(uri, event) != null) {
                LOG.debug("{} - Discovery agent generated a duplicate onServiceAdd event for: {}",getName(), uri);
                return;
            }

            URI connectUri = uri;
            try {
                connectUri = URISupport.applyParameters(connectUri, parameters, DISCOVERED_OPTION_PREFIX);
            } catch (URISyntaxException e) {
                LOG.warn("{} - could not apply query parameters: {} to: {}",getName(),parameters, connectUri, e);
            }

            LOG.info("{} - Establishing network connection from {} to {}", getName(), localURI, connectUri);

            Transport remoteTransport;
            Transport localTransport;
            try {
                // Allows the transport to access the broker's ssl configuration.
                if (getSslContext() != null) {
                    SslContext.setCurrentSslContext(getSslContext());
                } else {
                    SslContext.setCurrentSslContext(getBrokerService().getSslContext());
                }
                try {
                    remoteTransport = TransportFactory.connect(connectUri);
                } catch (Exception e) {
                    LOG.warn("{} - Could not connect to remote URI: {}: {}", getName(), connectUri, e.getMessage());
                    LOG.debug("{} - Connection failure exception: ", getName(), e);
                    try {
                        discoveryAgent.serviceFailed(event);
                    } catch (IOException e1) {
                        LOG.debug("{} - Failure while handling create remote transport failure event: {}", getName(), e1.getMessage(), e1);
                    }
                    return;
                }
                try {
                    localTransport = createLocalTransport();
                } catch (Exception e) {
                    ServiceSupport.dispose(remoteTransport);
                    LOG.warn("{} - Could not connect to local URI: {}: {}", getName(), localURI, e.getMessage());
                    LOG.debug("{} - Connection failure exception: ", getName(), e);

                    try {
                        discoveryAgent.serviceFailed(event);
                    } catch (IOException e1) {
                        LOG.debug("{} - Failure while handling create local transport failure event: {}", getName(), e1.getMessage(), e1);
                    }
                    return;
                }
            } finally {
                SslContext.setCurrentSslContext(null);
            }
            NetworkBridge bridge = createBridge(localTransport, remoteTransport, event);
            try {
                synchronized (bridges) {
                    bridges.put(uri, bridge);
                }
                bridge.start();
            } catch (Exception e) {
                ServiceSupport.dispose(localTransport);
                ServiceSupport.dispose(remoteTransport);
                LOG.warn("{} - Could not start network bridge between: {} and: {} due to: {}",getName(), localURI, uri, e.getMessage());
                LOG.debug("{} - Start failure exception: ", getName(),e);
                try {
                    // Will remove bridge and active event.
                    discoveryAgent.serviceFailed(event);
                } catch (IOException e1) {
                    LOG.debug("{} - Discovery agent failure while handling failure event: {}",getName(), e1.getMessage(), e1);
                }
            }
        }
    }

    @Override
    public void onServiceRemove(DiscoveryEvent event) {
        String url = event.getServiceName();
        if (url != null) {
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                LOG.warn("{} - Could not connect to remote URI: {} due to bad URI syntax: ", getName(), url, e);
                return;
            }

            // Only remove bridge if this is the active discovery event for the URL.
            if (activeEvents.remove(uri, event)) {
                synchronized (bridges) {
                    bridges.remove(uri);
                }
            }
        }
    }

    public DiscoveryAgent getDiscoveryAgent() {
        return discoveryAgent;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
        if (discoveryAgent != null) {
            this.discoveryAgent.setDiscoveryListener(this);
            // Set the connector name on the discovery agent if it supports it
            try {
                java.lang.reflect.Method setNameMethod = discoveryAgent.getClass().getMethod("setName", String.class);
                setNameMethod.invoke(discoveryAgent, getName());
            } catch (Exception e) {
                // Ignore if the discovery agent doesn't support setName
            }
        }
    }

    @Override
    protected void handleStart() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("You must configure the 'discoveryAgent' property");
        }
        this.discoveryAgent.start();
        super.handleStart();
    }

    @Override
    protected void handleStop(ServiceStopper stopper) throws Exception {
        for (Iterator<NetworkBridge> i = bridges.values().iterator(); i.hasNext();) {
            NetworkBridge bridge = i.next();
            try {
                bridge.stop();
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        bridges.clear();
        activeEvents.clear();
        try {
            this.discoveryAgent.stop();
        } catch (Exception e) {
            stopper.onException(this, e);
        }

        super.handleStop(stopper);
    }

    protected NetworkBridge createBridge(Transport localTransport, Transport remoteTransport, final DiscoveryEvent event) {
        class DiscoverNetworkBridgeListener extends MBeanNetworkListener {

            public DiscoverNetworkBridgeListener(BrokerService brokerService, ObjectName connectorName) {
                super(brokerService, DiscoveryNetworkConnector.this, connectorName);
            }

            @Override
            public void bridgeFailed() {
                if (!serviceSupport.isStopped()) {
                    try {
                        LOG.info("{} - bridgeFailed() calling discoveryAgent.serviceFailed() for event: {}", getName(), event);
                        discoveryAgent.serviceFailed(event);
                        LOG.info("{} - bridgeFailed() discoveryAgent.serviceFailed() completed successfully", getName());
                    } catch (IOException e) {
                        LOG.error("{} - bridgeFailed() discoveryAgent.serviceFailed() threw IOException for event: {}", getName(), event, e);
                    }
                } else {
                    LOG.info("{} - bridgeFailed() skipped because serviceSupport.isStopped()=true", getName());
                }

            }
        }
        NetworkBridgeListener listener = new DiscoverNetworkBridgeListener(getBrokerService(), getObjectName());

        DemandForwardingBridge result = getBridgeFactory().createNetworkBridge(this, localTransport, remoteTransport, listener);
        result.setBrokerService(getBrokerService());
        return configureBridge(result);
    }

    @Override
    public String toString() {
        return "DiscoveryNetworkConnector:" + getName() + ":" + getBrokerService();
    }
}
