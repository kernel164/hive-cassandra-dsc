package org.apache.cassandra.hive.cql3;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CREATE EXTERNAL TABLE test_kv (key INT, value STRING) STORED BY 'org.apache.cassandra.hive.cql3.Cql3StorageHandler' WITH SERDEPROPERTIES ("hosts" = "cass01.cfa00.net", "ks" = "cyberflow", "cf" = "test_kv", "columns.mapping" = "key,value" ); 
// CREATE EXTERNAL TABLE test_kv (key INT, value STRING) STORED BY 'org.apache.cassandra.hive.cql3.Cql3StorageHandler' WITH SERDEPROPERTIES ("hosts" = "cass01.cfa00.net", "ks" = "cyberflow" );
public class Cql3StorageHandler implements HiveStorageHandler {
    static Logger LOG = LoggerFactory.getLogger(Cql3StorageHandler.class);
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        LOG.info("TBL-PROPERTIES - {}", tableDesc.getProperties());
        HiveCassandraUtils.copyProperties(tableDesc.getProperties(), jobProperties, //
                HiveCassandraUtils.HOSTS, HiveCassandraUtils.KEYSPACE, //
                HiveCassandraUtils.COLUMN_FAMILY, HiveCassandraUtils.COLUMN_MAPPINGS);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        HiveCassandraUtils.copyProperties(tableDesc.getProperties(), jobProperties, //
                HiveCassandraUtils.HOSTS, HiveCassandraUtils.KEYSPACE, //
                HiveCassandraUtils.COLUMN_FAMILY, HiveCassandraUtils.COLUMN_MAPPINGS);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        HiveCassandraUtils.copyProperties(tableDesc.getProperties(), jobProperties, //
                HiveCassandraUtils.HOSTS, HiveCassandraUtils.KEYSPACE, //
                HiveCassandraUtils.COLUMN_FAMILY, HiveCassandraUtils.COLUMN_MAPPINGS);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return Cql3InputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new Cql3MetaHook();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return Cql3OutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return Cql3SerDe.class;
    }

    /**
     * 
     * @author dhamu
     */
    private class Cql3MetaHook implements HiveMetaHook {

        @Override
        public void commitCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void commitDropTable(Table tbl, boolean deleteData) throws MetaException {
            // boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
            // nothing to do...
        }

        @Override
        public void preCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void preDropTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void rollbackCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void rollbackDropTable(Table tbl) throws MetaException {
            // nothing to do...
        }
    }
}
