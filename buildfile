repositories.remote << 'http://alm-build:8080/nexus/content/groups/public/'

SLF4J = "org.slf4j:slf4j-api:jar:1.6.1"
LOGGING = group(
    "logback-classic",
    "logback-core",
    :under => "ch.qos.logback", :version => "1.0.0") <<
    SLF4J

GUAVA = "com.google.guava:guava:jar:12.0"

CASSANDRA = group("cassandra-all", "cassandra-thrift", :under => "org.apache.cassandra", :version => "1.0.6")
CASSANDRA_TEST = "org.yaml:snakeyaml:jar:1.6",
    "com.github.stephenc.high-scale-lib:high-scale-lib:jar:1.1.1",
    "com.github.stephenc:jamm:jar:0.2.5",
    "com.googlecode.concurrentlinkedhashmap:concurrentlinkedhashmap-lru:jar:1.2",
    "org.antlr:antlr-runtime:jar:3.2",
    "org.apache.cassandra.deps:avro:jar:1.4.0-cassandra-1"

HECTOR = "org.hectorclient:hector-core:jar:1.1-0",
    "org.apache.thrift:libthrift:jar:0.6.1",
    "commons-lang:commons-lang:jar:2.4",
    "commons-pool:commons-pool:jar:1.5.3",
    CASSANDRA, GUAVA

COMMONS = "commons-logging:commons-logging:jar:1.1.1",
          "commons-beanutils:commons-beanutils:jar:1.8.3"

define 'create' do
  project.version = '0.1'
  package :jar
  compile.with LOGGING, HECTOR, COMMONS
  run.using :main => ["com.rallydev.cassandra.CassandraBigData", "-create"]
end

