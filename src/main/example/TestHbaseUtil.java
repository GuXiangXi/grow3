

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
/*
 * author: DahongZhou
 * Date:
 */

public class TestHbaseUtil {


    public static void main(String[] args) throws IOException {
        String tableName = "testUtil";
        // 1.创建表表
        HBaseUtil.createTable(tableName, "base_info", "other_info");
        // 2.插入数据
        HBaseUtil.put(tableName, "bigdata_01_001", "base_info", "username", "zhangsan");
        HBaseUtil.put(tableName, "bigdata_01_001", "base_info", "age", "18");
        HBaseUtil.put(tableName, "bigdata_01_001", "base_info", "address", "深圳");
        HBaseUtil.put(tableName, "bigdata_01_001", "base_info", "sex", "男");
        HBaseUtil.put(tableName, "bigdata_01_001", "other_info", "hobbies", "basketball");
        HBaseUtil.put(tableName, "bigdata_01_001", "other_info", "salary", "2800.00");
        HBaseUtil.put(tableName, "bigdata_01_001", "other_info", "profession", "BigData");

        HBaseUtil.put(tableName, "bigdata_01_002", "base_info", "username", "lisi");
        HBaseUtil.put(tableName, "bigdata_01_002", "base_info", "age", "20");
        HBaseUtil.put(tableName, "bigdata_01_002", "base_info", "address", "广州");
        HBaseUtil.put(tableName, "bigdata_01_002", "base_info", "sex", "男");
        HBaseUtil.put(tableName, "bigdata_01_002", "other_info", "hobbies", "football");
        HBaseUtil.put(tableName, "bigdata_01_002", "other_info", "salary", "3000.00");
        HBaseUtil.put(tableName, "bigdata_01_002", "other_info", "profession", "Java");

        // 3.查询数据
        ArrayList<Map<String, String>> user1 = HBaseUtil.get(tableName, "bigdata_01_001", null, null);
        for (Map<String, String> map : user1) {
            System.out.println(map);
        }
        System.out.println("--------------------------------");

        //4.根据值来查询
        System.out.println("-------------------根据值来查询-----------------");
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("zhangsan"));
        ArrayList<ArrayList<Map<String, String>>> lists = HBaseUtil.scan(tableName, null, null, filter);
        for (ArrayList<Map<String, String>> list : lists) {
            for (Map<String, String> map : list) {
                System.out.println(map);
            }
        }

        //测试表的行数统计


//            String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.RegionObserver";
//            HBaseUtil.addTableCoprocessor("user", coprocessorClassName);
//            long rowCount = HBaseUtil.rowCount("user", "info");
        long rowCount = HBaseUtil.rowCount(tableName);
        System.out.println("rowCount: " + rowCount);
        System.exit(0);
    }
}
