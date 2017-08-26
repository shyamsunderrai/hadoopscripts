import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Stolen by srai
 */
public class HiveJdbc
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    //String tableName = "test";
    //ResultSet res = null;

    public static void main(String[] args) throws SQLException, ClassNotFoundException
    {

       try {
         org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
         conf.set("hadoop.security.authentication", "Kerberos");
         UserGroupInformation.setConfiguration(conf);
         //UserGroupInformation.loginUserFromKeytab("hive/xlautomation-2.h.c@H.C", "/etc/security/keytabs/hive.service.keytab");
         UserGroupInformation.loginUserFromSubject(null);
         Class.forName("org.apache.hive.jdbc.HiveDriver");
         System.out.println("getting connection");
         Connection con = DriverManager.getConnection("jdbc:hive2://xlautomation-2.h.c:10000/default;principal=hive/xlautomation-2.h.c@H.C", "hive", "");
         Statement stmt = con.createStatement();
         System.out.println("got connection");

         String tableName = "test";
         ResultSet res = null;

            res = stmt.executeQuery("select * from test limit 0");
            ResultSetMetaData m = res.getMetaData();
            int columnCount = m.getColumnCount();
            for(int i = 1;i<= columnCount;i++)
            {
                System.out.print("Column Name:" + m.getColumnName(i) + "\n");
                System.out.print("Column Type:" + m.getColumnTypeName(i) + "\n");
                System.out.print("Column Size:" + m.getColumnDisplaySize(i) + "\n");
                System.out.print("Column Type:" + m.getColumnClassName(i) + "\n");
 		
		System.out.println("---");
            }

            ResultSetMetaData k = res.getArray();
            for(int i = 1;i<= columnCount;i++)
            {
                System.out.print("Column Type:" + k.getColumnTypeName(i) + "\n");
                System.out.println("---");
            }             



       }
       catch (Exception e) {
         e.printStackTrace();
       }

    }
}
