package org.apache.cassandra.hive.cql3;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HiveCassandraUtils {
    private static Map<String, CassandraCluster> CASSANDRA_CLUSTERS = Maps.newConcurrentMap();
    public static final String COLUMN_MAPPINGS = "columns.mapping";
    public static final String COLUMN_FAMILY = "cf";
    public static final String HOSTS = "hosts";
    public static final String READ_CONSISTENCY_LEVEL = "read-consistency-level";
    public static final String KEYSPACE = "ks";

    public static String getPropertyValue(Properties props, String... propertyNames) {
        for (String propertyName : propertyNames) {
            Object value = props.get(propertyName);
            if (value != null)
                return value.toString();
        }
        return null;
    }

    public static String getConfigValue(Configuration conf, String... propertyNames) {
        for (String propertyName : propertyNames) {
            String value = conf.get(propertyName);
            if (value != null)
                return value;
        }
        return null;
    }

    public static void copyProperties(Properties properties, Map<String, String> jobProperties, String... names) {
        for (String name : names) {
            Object value = properties.get(name);
            if (value != null)
                jobProperties.put(name, value.toString());
            if (value == null && name == COLUMN_FAMILY) {
                jobProperties.put(name, properties.getProperty("name").split(".")[1]);
            }
        }
    }

    public static String getKeyspace(Configuration conf) {
        return getConfigValue(conf, HiveCassandraUtils.KEYSPACE);
    }

    public static String getColumnFamily(Configuration conf) {
        return getConfigValue(conf, HiveCassandraUtils.COLUMN_FAMILY);
    }

    public static String getInputConsistencyLevel(Configuration conf) {
        String value = getConfigValue(conf, HiveCassandraUtils.READ_CONSISTENCY_LEVEL);
        if (value == null)
            return "ONE";
        return value;
    }

    public static class CassandraCluster {
        static Logger LOG = LoggerFactory.getLogger(CassandraCluster.class);

        /** The Cassandra cluster instance. */
        protected Cluster cluster;
        protected Session session;
        private Configuration conf;

        public CassandraCluster(Configuration conf) {
            this.conf = conf;
        }

        public void init() {

            // final int maxRequestsPerConnection = 128;
            int maxConnections = 0; // getCassandraOptions().getConnectionsPerHost();
            PoolingOptions poolingOptions = null;
            if (maxConnections > 0) {
                poolingOptions = new PoolingOptions();
                // poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, getCassandraOptions().getMaxSimultaneousRequestsPerConnection());
                poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
                poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
            }

            SocketOptions socketOptions = new SocketOptions();
            socketOptions.setTcpNoDelay(true);
            socketOptions.setKeepAlive(true);
            // socketOptions.setConnectTimeoutMillis(getCassandraOptions().getConnectTimeoutInSecs() * 1000); // 10 secs
            // socketOptions.setReadTimeoutMillis(getCassandraOptions().getReadTimeoutInSecs() * 1000); // 20 secs

            // build cluster using the available list of cassandra hosts.
            String[] hostsList = HiveCassandraUtils.getConfigValue(conf, HiveCassandraUtils.HOSTS).split(","); // getCassandraOptions().getHosts();
            LOG.debug("Cassandra seeds (randomized): {}", (Object[]) hostsList);
            Cluster.Builder cb = new Cluster.Builder().addContactPoints(hostsList);
            if (poolingOptions != null)
                cb.withPoolingOptions(poolingOptions);
            cb.withSocketOptions(socketOptions);

            cb.withReconnectionPolicy(new ExponentialReconnectionPolicy(10, 10000)); // start at 10 millis for retries, exponentialy grow up to 10 seconds and then error
            cb.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())); // LatencyAwarePolicy
            cb.withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE)); // logs out when retrying
            cluster = cb.build();

            // if (getCassandraOptions().isCompression())
            // cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.SNAPPY);

            // connect to cluster.
            session = cluster.connect();
        }

        public ResultSet execute() {
            Select selectQuery = QueryBuilder.select().all().from(HiveCassandraUtils.getKeyspace(conf), HiveCassandraUtils.getColumnFamily(this.conf));
            selectQuery.setConsistencyLevel(ConsistencyLevel.valueOf(HiveCassandraUtils.getInputConsistencyLevel(conf)));
            return session.execute(selectQuery);
        }

        public Iterator<Map<String, Object>> asMapIterator() {
            ResultSet rs = execute();

            return new RowIterator<Map<String, Object>>(rs.iterator()) {
                @Override
                public Map<String, Object> next() {
                    return asMap(it.next());
                }
            };
        }

        public Iterator<List<Object>> asListIterator() {
            ResultSet rs = execute();

            return new RowIterator<List<Object>>(rs.iterator()) {
                @Override
                public List<Object> next() {
                    return asList(it.next());
                }
            };
        }

        abstract class RowIterator<T> implements Iterator<T> {
            protected Iterator<Row> it;

            public RowIterator(Iterator<Row> iterator) {
                this.it = iterator;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public void remove() {
            }
        }

        protected List<Object> asList(Row row) {
            if (row == null)
                return null;
            List<ColumnDefinitions.Definition> columns = row.getColumnDefinitions().asList();
            List<Object> list = Lists.newArrayListWithCapacity(columns.size());
            for (ColumnDefinitions.Definition column : columns)
                list.add(getObject(row, column.getName(), column.getType()));
            return list;
        }

        /**
         * row as map.
         */
        protected Map<String, Object> asMap(Row row) {
            if (row == null)
                return null;
            List<ColumnDefinitions.Definition> columns = row.getColumnDefinitions().asList();
            Map<String, Object> map = Maps.newHashMapWithExpectedSize(columns.size());
            for (ColumnDefinitions.Definition column : columns) {
                String name = column.getName();
                map.put(name, getObject(row, name, column.getType()));
            }
            return map;
        }

        /**
         * get column value
         */
        protected Object getObject(Row row, String name, DataType type) {
            switch (type.getName()) {
            case ASCII:
                return edit(name, row.getString(name));
            case BIGINT:
                return edit(name, row.getLong(name));
            case BLOB:
                return edit(name, row.getBytes(name));
            case BOOLEAN:
                return edit(name, row.getBool(name));
            case COUNTER:
                return edit(name, row.getLong(name));
            case CUSTOM:
                break;
            case DECIMAL:
                return edit(name, row.getDecimal(name));
            case DOUBLE:
                return edit(name, row.getDouble(name));
            case FLOAT:
                return edit(name, row.getFloat(name));
            case INET:
                return edit(name, row.getInet(name));
            case INT:
                return edit(name, row.getInt(name));
            case LIST:
                return edit(name, row.getList(name, type.getTypeArguments().get(0).asJavaClass()));
            case MAP:
                return edit(name, row.getMap(name, type.getTypeArguments().get(0).asJavaClass(), type.getTypeArguments().get(1).asJavaClass()));
            case SET:
                return edit(name, row.getSet(name, type.getTypeArguments().get(0).asJavaClass()));
            case TEXT:
                return edit(name, row.getString(name));
            case TIMESTAMP:
                return edit(name, row.getDate(name));
            case TIMEUUID:
                return edit(name, row.getUUID(name));
            case UUID:
                return edit(name, row.getUUID(name));
            case VARCHAR:
                return edit(name, row.getString(name));
            case VARINT:
                return edit(name, row.getVarint(name));
            default:
                break;
            }

            return null;
        }

        private Object edit(String name, Object value) {
            return value;
        }

        public void close() {
            LOG.info("Disconnecting Cassandra!");
            cluster.close();
        }
    }

    public static Iterator<Map<String, Object>> getRecordsAsMapIterator(Configuration conf) {
        CassandraCluster cluster = getCassandraCluster(conf);
        if (cluster != null)
            return cluster.asMapIterator();
        return newCassandraCluster(conf).asMapIterator();
    }

    public static Iterator<List<Object>> getRecordsAsListIterator(Configuration conf) {
        CassandraCluster cluster = getCassandraCluster(conf);
        if (cluster != null)
            return cluster.asListIterator();
        return newCassandraCluster(conf).asListIterator();
    }

    public static CassandraCluster newCassandraCluster(Configuration conf) {
        CassandraCluster cluster = new CassandraCluster(conf);
        CASSANDRA_CLUSTERS.put(getConfigValue(conf, HOSTS), cluster);
        cluster.init();
        return cluster;
    }

    public static CassandraCluster getCassandraCluster(Configuration conf) {
        return CASSANDRA_CLUSTERS.get(getConfigValue(conf, HOSTS));
    }

    public static void closeCassandraCluster(Configuration conf) {
        CassandraCluster cluster = getCassandraCluster(conf);
        if (cluster != null)
            cluster.close();
    }
}
