
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;


public class HiveMetaStoreClient {
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
    //HiveMetaStore的客户端
    private static final String PARQUET = "parquet";
    private static final String TEXT = "text";
    private static final String ORC = "orc";

    private IMetaStoreClient client;

    public HiveMetaStoreClient() {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.addResource("hive-site.xml");
            this.client = RetryingMetaStoreClient.getProxy(hiveConf);
        } catch (MetaException ex) {
            this.logger.error(ex.getMessage());
        }
    }

    public Database getDatabase(String db) {
        Database database = null;
        try {
            database = this.client.getDatabase(db);
        } catch (TException ex) {
            this.logger.error(ex.getMessage());
        }
        return database;
    }

    public void close() {
        this.client.close();
    }
}
