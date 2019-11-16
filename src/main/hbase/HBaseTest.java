

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseTest {

    HBaseAdmin admin = null;//表的管理类
    HTable table = null;//数据的管理类
    String tname = "phone3";//表名

    //完成初始化
    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node0003");
        admin = new HBaseAdmin(conf);
        table = new HTable(conf, tname.getBytes());
    }

    //建表
    @Test
    public void createTable() throws Exception {
        //表的描述类
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("tb55"));
        //列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("cf".getBytes());
        desc.addFamily(family);
        if (admin.tableExists(tname)) {
            admin.disableTable(tname);
            admin.deleteTable(tname);
        }
        admin.createTable(desc);
    }

    @Test
    public void insert() throws Exception {
        Put put = new Put("1111".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.add("cf".getBytes(), "age".getBytes(), "12".getBytes());
        put.add("cf".getBytes(), "sex".getBytes(), "man".getBytes());
        table.put(put);
    }

    //添加要获取的列和列族，减少网络的io，相当于在服务器端做了过滤
    @Test
    public void get() throws Exception {
        Get get = new Get("1111".getBytes());

        get.addColumn("cf".getBytes(), "name".getBytes());
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());
        Result result = table.get(get);
        Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
        Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell2)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell3)));
    }

    @Test
    public void scan() throws Exception {
        Scan scan = new Scan();
        ResultScanner rss = table.getScanner(scan);
        for (Result result : rss) {
            Cell ce1 = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
            Cell ce2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
            Cell ce3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
            System.out.println(Bytes.toString(CellUtil.cloneValue(ce1)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(ce2)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(ce3)));
        }
    }

    @After
    public void destory() throws Exception {
        if (admin != null) {
            admin.close();
        }
    }
}
