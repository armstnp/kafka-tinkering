# Environment Provisioning #

## Kafka Brokers ##

### References ###
* https://kafka.apache.org/documentation#operations
* http://docs.confluent.io/3.1.1/kafka/deployment.html
* http://www.cloudera.com/documentation/kafka/latest/topics/kafka_performance.html

### Hardware ###
* Likely best if isolated
* Disk
  * **Throughput** is vital
  * Keep partition storage on its own disks
  * HDD is okay, as reads and writes are primarily sequential and not random-access
  * XFS Filesys recommended
* Memory
  * Varying formulas for back-of-the-envelope needs:
    * `replica.fetch.max.bytes * <# of partitions replicated>`
    * `<write throughput> * <~ minimum buffer time>`
* Network
   * Low latency between nodes
   * High bandwidth

### Software ###
* JRE 8 (_RPMs available on Oracle download page_)
* Scala Runtime (see version of Kafka for min. version required) (_RPMs available_)
* Will need to create an RPM for the brokers themselves

-----

## Zookeeper Instances ##

### References ###
* https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.htm
* https://kafka.apache.org/documentation#zkops

### Hardware ###
* Focus on physical / hardware redundancy
* _"At Yahoo!, ZooKeeper is usually deployed on dedicated RHEL boxes, with dual-core processors, 2GB of RAM, and 80GB IDE hard drives."_
* Disk
  * Separate transaction logs and snapshots into separate disk groups.
  * Snapshots can safely go on the same disk as other resources (e.g. OS).
* Memory
  * Sufficient to fit the JVM heap space without swapping - e.g. low needs.
* Network
  * Low latency between nodes

### Software ###
* JRE 8 (_RPMs available on Oracle download page_)
* Prefer to use the Zookeeper JAR and run scripts packaged alongside Kafka.

-----

## Kafka Clients ##

### General Notes ###
* Each type of producer and consumer created will likely have its own needs. It will be vital to have responsive tuning based on gathered metrics, as horizontal scaling - especially for consumers - is not necessarily possible or useful.

### Producers ###
* Likely primarily bandwidth-constrained
* Needs enough memory to buffer output until acknowledged by the Kafka brokers.

### Consumers ###
* Bandwidth-constrained
* Needs enough memory to hold and operate on message batches.

-----

## Kafka Manager ##

### References ###
* https://github.com/yahoo/kafka-manager

### Hardware ###
* CPU
  * More CPU cores should help the manager scale up alongside the cluster.
* Memory and disk may not be great concerns, as responsiveness requirements are low.
* The web app's zookeeper instance can be kept on the same hardware or a separate box, or it could use a namespace of the existing Kafka cluster instances.

### Software ###
* Java 8+
* Application can be built to an RPM package on a Scala build machine w/ sbt

------

## System Metrics Agents (collectd) ##

### References ###
* RPM packages available (https://collectd.org/download.shtml#rpm)

### Hardware ###
* By definition, this must reside on the machine of interest.  Ought to be low-needs.

-----

## Prometheus ##

### References ###
* https://prometheus.io/docs/operating/storage/
* https://prometheus.io/docs/operating/federation/
* Possibly https://github.com/utobi/prometheus-rpm for RPM support

### Hardware ###
* Memory
  * Will likely use whatever is thrown at it.  Start medium and scale?
* Disk
  * Depends on network throughput and configured retention time.  Start medium and scale?
* Network
  * High download desirable
* In the circumstances that one instance no longer scales, multiple machines may be required in order to create a federated Prometheus cluster.

-----

## JMX / collectd ➡ Prometheus Exporter ##

### References ###
* https://github.com/prometheus/jmx_exporter
* https://github.com/prometheus/collectd_exporter

### Hardware ###
* Currently presuming low-needs - high network throughput is desirable.
* Current plan - cohost with exposed applications, presuming hardware needs are fairly minimal.
* If this becomes infeasible, migrate agents to the Prometheus scraping machine, or distribute them across their own set of machines.

### Software ###
* Likely Java 8+
* Will need to create a custom installation RPM

-----

## Grafana ##

### References ###
* http://docs.grafana.org/installation/rpm/

### Hardware ###
* Unknown needs!!
* May be able to start by cohosting with Prometheus, moving to its own machine if necessary.
