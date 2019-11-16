import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class hdfs1 {
    public static void main(String[] args) {
        String path="hdfs://192.168.137.7:8020";
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS",path);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String targetPath="/crawl/"+simpleDateFormat.format(new Date())+"/qxb2";
        Path path1 = new Path(targetPath);
        Path path2 = new Path("E:\\bigdata-test\\jsonData.json");

        try {
            FileSystem fileSystem=FileSystem.get(entries);
            fileSystem.	copyFromLocalFile(path2,path1);
            //fileSystem.copyToLocalFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
