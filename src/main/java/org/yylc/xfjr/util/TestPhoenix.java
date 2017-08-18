package org.yylc.xfjr.util;


//import com.salesforce.phoenix.jdbc.PhoenixConnection;

public class TestPhoenix {


    /*public static void main(String[] args) {
        PhoenixConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
            conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:vnode121,vnode122,vnode123");
            conn.setAutoCommit(false);

            int upsertBatchSize = conn.getMutateBatchSize();
            String upsertStatement = "upsert into youku values(?,?,?,?)";
            stmt = conn.prepareStatement(upsertStatement);
            int rowCount = 0;
            for (int i = 0; i < 100000000; i++) {

                Random r = new Random();

                int d = r.nextInt(1000);

                String id = "id" + i;
                String name = "name" + d;
                int click = r.nextInt(100);
                float time = r.nextFloat() * 100;
                stmt.setString(1, id);
                stmt.setString(2, name);
                stmt.setInt(3, click);
                stmt.setFloat(4, time);
                stmt.execute();
                // Commit when batch size is reached
                if (++rowCount % upsertBatchSize == 0) {
                    conn.commit();
                    System.out.println("Rows upserted: " + rowCount);
                }
            }
            conn.commit();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }*/


}