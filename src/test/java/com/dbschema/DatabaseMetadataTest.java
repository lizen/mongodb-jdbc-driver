package com.dbschema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Liudmila Kornilova
 **/
public class DatabaseMetadataTest {
  private static Connection connection;

  private int[] _columnTypes;
  private String[] _columnNames;
  private String[] _columnLabels;

  private String dataType;
  private String length;
  private String name;
  private String nameInSource;
  private String nativeType;
  private String fixedLength;

  private String order;
  private String precision;
  private String scale;
  private String notNull;
  private String charLen;
  private String comment;

  @BeforeClass
  public static void before() throws SQLException {
    Properties properties = new Properties();

    //
    String url = "jdbc:mongodb://sample:sample@192.168.4.82:27017/sampledb?authSource=sampledb&authMechanism=SCRAM-SHA-1&ssl=false";
    //

    properties.setProperty("authSource", "sampledb");
    properties.setProperty("authMechanism", "SCRAM-SHA-1");
    properties.setProperty("ssl", "false");
    connection = new MongoJdbcDriver().connect(url, properties);

    //connection = new MongoJdbcDriver().connect(URL, properties);

  }

  @AfterClass
  public static void after() throws SQLException {
    if (connection != null) connection.close();
  }

  @Test
  public void test() throws SQLException {
    try {
//      connection.createStatement().execute("use my_database");
//      connection.createStatement().execute("db.getCollection('hello.my_collection').insertOne({'hello_world': 1})");
//      ResultSet columns = connection.getMetaData().getColumns("", "my\\_database", "hello.my\\_collection", "%");
        DatabaseMetaData dbmd = connection.getMetaData();

        String catalogNm = null;

        try (ResultSet rs = dbmd.getCatalogs()) {
            while (rs.next()) {
                catalogNm = rs.getString(1);
                System.out.println("catalog: " + catalogNm);
            }
        }

        String schemaNm = null;
        try (ResultSet rs = dbmd.getSchemas()) {
            while (rs.next()) {
                schemaNm = rs.getString(1);
                System.out.println("schema: " + schemaNm);
            }
        }

        List<String> tableNmList = new ArrayList<String>();
        try (ResultSet rs = dbmd.getTables(catalogNm, schemaNm, null, null)) {

            while (rs.next()) {
                tableNmList.add(rs.getString(3));
            }

            //ResultSetMetaData tblMetaData = rs.getMetaData();
            //int columnCount1 = tblMetaData.getColumnCount();

            for (String tableNm : tableNmList) {
                System.out.println("Collection(Table): " + tableNm);

                try (ResultSet colRs = dbmd.getColumns(catalogNm, schemaNm, tableNm, null)) {

                    while (colRs.next()) {

                        ResultSetMetaData metaData = colRs.getMetaData();

                        int columnCount = metaData.getColumnCount();
                        _columnTypes = new int[columnCount];
                        _columnNames = new String[columnCount];
                        _columnLabels = new String[columnCount];

                        for(int i = 1; i <= columnCount; i++) {
                            _columnTypes[i-1] = metaData.getColumnType(i);
                            _columnNames[i-1] = metaData.getColumnName(i).toLowerCase();
//                          System.out.println("_columnNames : "+ _columnNames[i-1]);
                            _columnLabels[i-1] = metaData.getColumnLabel(i).toLowerCase();
                        }

                        this.nameInSource = colRs.getString("COLUMN_NAME");
                        this.order = colRs.getString("ORDINAL_POSITION");
                        this.notNull = colRs.getString("IS_NULLABLE");
                        this.charLen = colRs.getString("CHAR_OCTET_LENGTH");   // 열의 최대 바이트 수
                        this.comment = colRs.getString("REMARKS");
                        this.length = String.valueOf(colRs.getInt("COLUMN_SIZE"));
                        int dataType = colRs.getInt("DATA_TYPE");

                        System.out.println("Collection(Column) :" + colRs.getString(1) + "-" + colRs.getString(2) + "-" + colRs.getString(3) + "-" + colRs.getString(4) + "/Data Type:" + dataType + "/Length:" + length);
                    }
                }
            }
        }

        connection.createStatement().execute("use sampledb");
        connection.createStatement().execute("db.central_park_weather.find({tmax: 16})");
        ResultSet columns = connection.getMetaData().getColumns("", "sampledb", "sampledb.central\\_park\\_weather", "%");

      assertTrue(columns.next());
      assertEquals("my_database", columns.getString(2));
      assertEquals("hello.my_collection", columns.getString(3));
      assertEquals("_id", columns.getString(4));
      assertTrue(columns.next());
      assertEquals("hello_world", columns.getString(4));

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connection.createStatement().execute("db.getCollection('hello.my_collection').drop()");
    }
  }
}
