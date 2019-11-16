

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.util.StopWatch;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * 创建表、删除表、新增数据、查询数据（get、scan、scan filter）
 * author: DahongZhou
 * Date:
 */
public class HBaseUtil {
    private static Connection connection;

    //scan表数据
    public static ArrayList<ArrayList<Map<String, String>>> scan(String tName, String startKey, String endKey, Filter filter) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Scan scan = new Scan();
        if (startKey != null && endKey != null) {
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(endKey));
        }
        if (filter != null) {
            scan.setFilter(filter);
        }
        Result result = null;
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<ArrayList<Map<String, String>>> arrayLists = new ArrayList<ArrayList<Map<String, String>>>();
        while ((result = scanner.next()) != null) {
            ArrayList<Map<String, String>> value = getValue(result);
            arrayLists.add(value);
        }
        return arrayLists;
    }

    /**
     * 查询方法
     * 封装了get方法，得到get的对象结果
     *
     * @param tName  表名
     * @param rowkey 行键值
     * @param cf     列簇名
     * @param field  列名，字段
     */
    public static ArrayList<Map<String, String>> get(String tName, String rowkey, String cf, String field) throws IOException {

        // user,rowkey,cf:username:value
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(5);  //一次性获取多个版本数据
        if (cf != null) {
            get.addFamily(Bytes.toBytes(cf));
        }
        if (field != null && cf != null) {
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(field));
        }
        Result result = table.get(get);
        return getValue(result);
    }

    //获取表的数据：获取get对象里面的值
    private static ArrayList<Map<String, String>> getValue(Result result) {
        ArrayList<Map<String, String>> maps = new ArrayList<Map<String, String>>();
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            HashMap<String, String> hashMap = new HashMap<String, String>();
            long timestamp = cell.getTimestamp();
            System.out.println(timestamp);
            hashMap.put("RowKey", Bytes.toString(CellUtil.cloneRow(cell)));
            hashMap.put("Family", Bytes.toString(CellUtil.cloneFamily(cell)));
            hashMap.put("Field", Bytes.toString(CellUtil.cloneQualifier(cell)));
            hashMap.put("Value", Bytes.toString(CellUtil.cloneValue(cell)));
            maps.add(hashMap);
        }
        return maps;
    }


    /**
     * 得到一个put方法
     * 这个方法不支持大量数据的传入
     *
     * @param tName  表名
     * @param rowkey 行键值
     * @param cf     列簇名
     * @param filed  字段名，列名
     * @param value  列的值
     * @throws IOException
     */
    public static void put(String tName, String rowkey, String cf, String filed, String value) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(filed), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 该api提供的创建表的方式，必须是表不存在。
     * 如果表存在，就需要hbase的管理员去通过hbase shell命令去删除表。
     *
     * @throws IOException
     */
    @Test
    public static void createTable(String tName, String... familyNames) throws IOException {
        Admin admin = getConnection().getAdmin();
        TableName tableName = TableName.valueOf(tName);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String familyName : familyNames) {
                hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
            }
            admin.createTable(hTableDescriptor);
        }
        admin.close();
    }

    /**
     * 获取hbase的连接
     *
     * @return
     * @throws IOException
     */
    private static Connection getConnection() throws IOException {
        if (connection == null) {
            Configuration config = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(config, Executors.newFixedThreadPool(30));
        }
        return connection;
    }


    //获取表的行数：方式一
    public static long rowCount(String tableName) {
//    public void rowCount(String tableName) {
        long rowCount = 0;
        try {
         /*   Configuration configuration = HBaseConfiguration.create();
            HTable table = new HTable(configuration, tableName);*/
            //或
            Table table = getConnection().getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                rowCount += result.size();
            }
        } catch (IOException e) {
//            logger.info(e.getMessage(), e);
        }
        return rowCount;


       /* long rowCount = 0;
        try {
            //计时
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            TableName name=TableName.valueOf(tableName);
            //connection为类静态变量
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            //FirstKeyOnlyFilter只会取得每行数据的第一个kv，提高count速度
            scan.setFilter(new FirstKeyOnlyFilter());

            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                rowCount += result.size();
            }

            stopWatch.stop();
            System.out.println("RowCount: " + rowCount);
            System.out.println("统计耗时：" +stopWatch.getTotalTimeMillis());
        } catch (Throwable e) {
            e.printStackTrace();
        }*/

    }


    //获取表的行数：方式二
    //使用hbase提供的聚合coprocessor协处理器（暂时还有点bug）

    /* 是什么原因让Coprocessor在统计Rowkey的个数上，拥有如此明显的优势呢？
     这是因为在Table注册了Coprocessor之后，在执行AggregationClient的时候，会将RowCount分散到Table的每一个Region上，Region内RowCount的计算，是通过RPC执行调用接口，由Region对应的RegionServer执行InternalScanner进行的。
     因此，性能的提升有两点原因:
             1) 分布式统计。将原来客户端按照Rowkey的范围单点进行扫描，然后统计的方式，换成了由所有Region所在RegionServer同时计算的过程。
             2）使用了在RegionServer内部执行使用了InternalScanner。这是距离实际存储最近的Scanner接口，存取更加快捷。
 */
    /*public static void addTableCoprocessor(String tableName, String coprocessorClassName) {
        try {
            //先disable表，添加协处理器后再enable表
            Admin admin = getConnection().getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
            htd.addCoprocessor(coprocessorClassName);
            admin.modifyTable(TableName.valueOf(tableName), htd);
            admin.enableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
//        logger.info(e.getMessage(), e);
        }
    }

    public static long rowCount(String tableName, String family) {
        Configuration configuration = HBaseConfiguration.create();
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
//            logger.info(e.getMessage(), e);
        }
        return rowCount;
    }
*/

}
