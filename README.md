# GoshawkDB Java Client

* Maven: Available in [maven central](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.goshawkdb%22)
* License: Apache License version 2.0 (see [LICENSE](LICENSE) file)
* Issue tracker: Please issue [GoshawkDB's bug tracker](https://bugs.goshawkdb.io/)

## Running the tests

The tests are all integration tests and need a working GoshawkDB
server to run against. Some of the tests may take some time to
run. The simplest way to get started is to
[install the server](https://goshawkdb.io/starting.html), then:


    $ cd src/test/resources
    $ goshawkdb -config defaultClusterConfig.json -cert defaultClusterKeyPair.pem


This will start a 1-node GoshawkDB cluster running on `localhost` with
the default port, using known certificates and key pairs. Now to run
the tests, just:


    $ ./gradlew test


This uses certificates that are checked into this repository, which
you will find in the [src/test/resources](src/test/resources)
folder. Obviously, these certificates and key pairs are public, so
please do not deploy them in production!

To set the authentication or customise the servers to which the tests
connect, there are three environment variables to set:

* `GOSHAWKDB_CLUSTER_HOSTS` This is a comma-separated list of hosts to
  which the tests will try to connect. They must all be part of the
  same cluster. If, for example, you happen to be running a three-node
  cluster all on `localhost` ports 10001, 10002 and 10003, you may
  wish to set this to
  `"localhost:10001,localhost:10002,localhost:10003"`. The default is
  `"localhost"`
* `GOSHAWKDB_CLUSTER_CERT` This is a path to a file containing
  the cluster X.509 certificate which the client uses to authenticate
  the server. This file should contain the cluster certificate only,
  and *not* the cluster key pair. The default is
  [src/test/resources/defaultClusterCert.pem](src/test/resources/defaultClusterCert.pem)
* `GOSHAWKDB_CLIENT_KEYPAIR` This is a path to a file
  containing a client X.509 certificate and key pair which the client
  uses to authenticate to the server. The default is
  [src/test/resources/defaultClientKeyPair.pem](src/test/resources/defaultClientKeyPair.pem)

If you set these in the terminal before invoking `gradlew test` then
they will be picked up.
