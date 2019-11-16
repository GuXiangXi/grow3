import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest2 {
    private Configuration conf = null;

    // 初始化
    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "namenode:2181,datanode:2181,datanode:2181");
    }

    // 创建表
    @Test
    public void testCreate() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 判断表是否存在，若存在则先删除，再重新创建
        boolean b = admin.tableExists(Bytes.toBytes("person"));
        if (b) {
            admin.disableTable(Bytes.toBytes("person"));
            admin.deleteTable("person");
        }

        TableName name = TableName.valueOf("person");
        HTableDescriptor table = new HTableDescriptor(name);

        // 添加2个列族
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extar_info = new HColumnDescriptor("extra_info");

        base_info.setMaxVersions(5);

        table.addFamily(base_info);
        table.addFamily(extar_info);
        admin.createTable(table);


    }

    // 删除表
    @Test
    public void testDrop() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("person");
        admin.deleteTable("person");
        admin.close();

    }

    // 向表中添加数据
    @Test
    public void testPut() throws Exception {
        HTable table = new HTable(conf, "person");
        Put p = new Put(Bytes.toBytes("rk_0002"));
        p.add("base_info".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        table.put(p);
        table.close();
    }

    // 查询表中数据
    @Test
    public void testGet() throws Exception {
        HTable table = new HTable(conf, "person");
        Get get = new Get(Bytes.toBytes("rk_0001"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey is :"
                    + new String(CellUtil.cloneRow(cell)) + "\t Family is :"
                    + new String(CellUtil.cloneFamily(cell))
                    + "\t Qualifier is :"
                    + new String(CellUtil.cloneQualifier(cell))
                    + "\t Value is :"
                    + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}