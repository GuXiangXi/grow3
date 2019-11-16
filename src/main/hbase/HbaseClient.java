import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseClient {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
    }

    public static boolean isExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName,String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        if (isExist(tableName)){
            System.out.println("表已经存在，请输入其它表名");
        }else{
            //2.注意，创建表的话 需要创建一个描述器
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

            //3.创建列族
            for (String cf : columnFamily) {
                htd.addFamily(new HColumnDescriptor(cf));
            }

            //4.创建表
            admin.createTable(htd);
            System.out.println("表已创建成功！");
        }
    }

    //3.删除HBase中的表
    public static void deleteTable(String tableName) throws IOException{
        //对表操作需要使用HBaseAdmin
        Connection connection = ConnectionFactory.createConnection(conf);
        //管理表
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //1.如果表存在 删除 否则打印不存在
        //需要先指定表不可用 再删除
        if (isExist(tableName)){
            //2.指定不可用
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }else {
            System.out.println("表不存在，请重新输入表名！");
        }
    }

    //4.添加数据put 'user','rowKey'
    public static void addRow(String tableName, String rowkey, String cf, String column, String value) throws IOException {
        //对表操作需要使用HBaseAdmin
        Connection connection = ConnectionFactory.createConnection(conf);
        //拿到表对象
        Table t = connection.getTable(TableName.valueOf(tableName));
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //1.用put方式加入数据
        Put p = new Put(Bytes.toBytes(rowkey));
        //2.加入数据
        p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        t.put(p);
    }

    public static void main(String[] args) throws IOException {
        System.out.println(isExist("emp11"));
        createTable("ssad","henshuai","feichangshuai");
        createTable("ssas","info");

        deleteTable("ccw");
        createTable("zxc","info");
        addRow("vvv","101","info","age","18");

    }
}