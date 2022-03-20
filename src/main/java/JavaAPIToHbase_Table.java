import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class JavaAPIToHbase_Table {
    public static Configuration configuration;
    public  static Admin admin;
    public  static Connection connection;
    static {
        //建立连接
        //1.创建配置
        configuration= HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorm","127.0.0.1");
        //zookeeper客户端的端号2181
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //hbase主默认端口是60000
        configuration.set("hbase.master","127.0.0.1:60000");
        //2.创建连接
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //3.获得一个建表、删表的对象，hbaseAdmin（）是继承admin（）
        try {
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    主函数
     */
    public static void main(String[] args) throws IOException
    {
        String familyNames[]={"name","info","score"};
        String tableName="chengxiaoping:student";

        //创建表chengxiaoping:student
        createTable(tableName,familyNames);
        //查看已有表listTables
        listTables();

        //插入数据 在表中插入一条数据，其行键为Tom,info:student_id为20210000000001（info，student_id为info下的子列）
        insertDataToTable(tableName,"Tom","info","student_id","20210000000001");
        insertDataToTable(tableName,"Tom","info","class","1");
        insertDataToTable(tableName,"Tom","score","understanding","75");
        insertDataToTable(tableName,"Tom","score","programming","82");

        insertDataToTable(tableName,"Jerry","info","student_id","20210000000002");
        insertDataToTable(tableName,"Jerry","info","class","1");
        insertDataToTable(tableName,"Jerry","score","understanding","85");
        insertDataToTable(tableName,"Jerry","score","programming","67");

        insertDataToTable(tableName,"Jack","info","student_id","20210000000003");
        insertDataToTable(tableName,"Jack","info","class","2");
        insertDataToTable(tableName,"Jack","score","understanding","80");
        insertDataToTable(tableName,"Jack","score","programming","80");

        insertDataToTable(tableName,"Rose","info","student_id","20210000000003");
        insertDataToTable(tableName,"Rose","info","class","2");
        insertDataToTable(tableName,"Rose","score","understanding","60");
        insertDataToTable(tableName,"Rose","score","programming","61");

        //查询表数据
        getData(tableName,"Rose","score","understanding");

        //按照指定条件删除数据,删除rose的score下面的understanding数据，一列
        //deleteTableByCondituon(tableName,"Rose","score","understanding");
        //按照指定条件删除数据,删除rose的score下面的understanding和programming数据，两列
        //deleteTableByCondituon(tableName,"Rose","score","");
        //删除整个表
        //dropTable(tableName);
    }

    /*
    关闭连接
     */
    public static void close()
    {
        try {
            if(admin !=null)
            {
                admin.close();
            }
            if(null!=connection)
            {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
    创建表
    tableName 表名
    familyNames 列族名
     */
    public static void createTable(String tableName, String[] familyNames)
            throws IOException
    {
        System.out.println("start create table ......");
        //判断表是否存在，如果存在表，就退出
        if(admin.tableExists(TableName.valueOf(tableName)))
        {
            System.out.println("Table is exists!");
            return ;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName:familyNames)
        {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }

        admin.createTable(tableDescriptor);
        System.out.println("createTable success!");
        System.out.println("end create table ......");
        //关闭连接
        close();
    }

    /*
    查看已有表数据
     */
    private static void listTables() throws IOException{
        HTableDescriptor hTableDescriptors[]=admin.listTables();
        for (HTableDescriptor hTableDescriptor:hTableDescriptors)
        {
            System.out.println(hTableDescriptor.getNameAsString());
        }
    }

    /*
     指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey  Tom
     * @param family 列族  info                       score
     * @param column 列    student_id       class     75
     * @param value        20210000000001   1         82
     */
    public static void insertDataToTable(String tableName, String rowKey, String family, String column,String value)
            throws IOException {
        System.out.println("start insert data ......");
        Table table=connection.getTable(TableName.valueOf(tableName));
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        connection.getTable(TableName.valueOf(tableName)).put(put);
        System.out.println("insertData " + rowKey + " totable " + tableName + "ok.");
        System.out.println("end insert data ......");
        close();
    }

    /*
    删除整个表
     */
    public static void dropTable(String tableName) throws IOException {
        //判断表是否存在，不存在是报异常
        if(admin.tableExists(TableName.valueOf(tableName)))
        {
            System.out.println(tableName+"不存在！");
            return;
        }

        //删除之前要将表先做disable
        if(!admin.isTableDisabled(TableName.valueOf(tableName)))
        {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("deleteTable" + tableName + "ok!");
        close();
    }

    /*
    按照指定条件删除数据
    * 删除数据
    * @param tableName 表名
    * @param rowKey 行键
    * @param colFamily 列族名
    * @param column 列名
     */
    public static void deleteTableByCondituon(String tableName, String rowKey, String family, String column)
    throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        Delete delete=new Delete(rowKey.getBytes());
        //删除指定列族的所有数据
        delete.addFamily(family.getBytes());
        //删除指定列的数据
        delete.addColumn(family.getBytes(),column.getBytes());
        close();
    }

    /*

     */
    public static void getData(String tableName, String rowKey, String family, String column) 
    throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get=new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(),column.getBytes());
        Result result=table.get(get);
        showCell(result);
        table.close();
        close();
    }

    /*
    格式化输出查询的数据
     */
    public static void showCell(Result result) {
        Cell[] cells=result.rawCells();
        for (Cell cell:cells)
        {
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
}
