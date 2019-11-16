import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class hdfs2 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.FATAL);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String path="hdfs://192.168.137.7:8020"+"/crawl/"+simpleDateFormat.format(new Date())+"/qxb2";
        SparkSession spark = SparkSession.builder().appName("sql").master("local[3]").config("spark.some.config.option", "some-value").getOrCreate();
        SQLContext sqlContext=new SQLContext(spark.sparkContext());
//        SparkConf conf = new SparkConf().setAppName("con").setMaster("local[5]");
        //JavaSparkContext sparkContext = new JavaSparkContext(conf);
//        JavaSparkContext sc = new JavaSparkContext(conf);

        //接收数据
        Dataset<Row> df = sqlContext.read().json(path);

//        JavaRDD<Row> rowJavaRDD = df.javaRDD();
//        JavaRDD<Row> distinct = rowJavaRDD.distinct();

        //建立视图
        df.dropDuplicates().createOrReplaceTempView("qxb2");
        spark.sql("select * from qxb2 where uni_cre_code!=' '").show(false);


        //使用list接收df
//        List list=df.collectAsList();
//        System.out.println(list.size());list.forEach(x-> System.out.println(x));

    }
}
