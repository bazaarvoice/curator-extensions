curator-extensions
==================

Helpers that extend the functionality of curator.

New Recipes
===========

```xml
<dependency>
    <groupId>com.bazaarvoice.curator</groupId>
    <artifactId>recipes</artifactId>
</dependency>
```

PersistentEphemeralNode
-----------------------

Ensures that as long as you have a connection to ZooKeeper that your ephemeral node stays around.

This recipe prevents node loss from:
- connection and session interruptions
- accidental deletion

NodeDiscovery
-------------

Works on top of ```PathChildrenCache``` to automatically parse the data portion of the node and notify listeners. See
```NodeDiscovery.NodeDataParser``` and ```NodeDiscovery.NodeListener``` for more info.

Easy Dropwizard Integration
===========================

```xml
<dependency>
    <groupId>com.bazaarvoice.curator</groupId>
    <artifactId>dropwizard</artifactId>
</dependency>
```

```java
public class SampleConfiguration extends Configuration {
    @Valid
    @NotNull
    @JsonProperty("zooKeeper")
    private ZooKeeperConfiguration _zooKeeperConfiguration = new ZooKeeperConfiguration();

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return _zooKeeperConfiguration;
    }
}

public class SampleService extends Service<SampleConfiguration> {
    public static void main(String[] args) throws Exception {
        new SampleService().run(args);
    }

    @Override
    public void initialize(... bootstrap) {
       // ...
    }

    @Override
    public void run(SampleConfiguration cfg, Environment env) {
        CuratorFramework curator = cfg.getZooKeeperConfiguration().newManagedCurator(env);

        environment.addHealthCheck(new CuratorHealthCheck(curator));
    }
}
```
