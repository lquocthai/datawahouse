package vn.edu.hcmuaf.fit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class LoadDateDim {

    private static final String DB_URL = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    private static final String CSV_FILE = "D:/Workspace/Datawarehouse/month_dim_date_dim_dw_20200626/Date_Dim/date_dim.csv";

    public static void main(String[] args) {
        String sql = "INSERT INTO date_dim (" +
                "date_sk, full_date, day_since_2005, month_since_2005, day_of_week, calendar_month, " +
                "calendar_year, calendar_year_month, day_of_month, day_of_year, week_of_year_sunday, " +
                "year_week_sunday, week_sunday_start, week_of_year_monday, year_week_monday, week_monday_start, " +
                "quarter_year, quarter_since_2005, holiday, day_type" +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(sql);
             BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {

            conn.setAutoCommit(false);

            String line;
            int batchSize = 0;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                ps.setInt(1, Integer.parseInt(fields[0]));         // date_sk
                ps.setString(2, fields[1]);                       // full_date
                ps.setInt(3, Integer.parseInt(fields[2]));
                ps.setInt(4, Integer.parseInt(fields[3]));
                ps.setString(5, fields[4]);
                ps.setString(6, fields[5]);
                ps.setInt(7, Integer.parseInt(fields[6]));
                ps.setString(8, fields[7]);
                ps.setInt(9, Integer.parseInt(fields[8]));
                ps.setInt(10, Integer.parseInt(fields[9]));
                ps.setInt(11, Integer.parseInt(fields[10]));
                ps.setString(12, fields[11]);
                ps.setString(13, fields[12]);
                ps.setInt(14, Integer.parseInt(fields[13]));
                ps.setString(15, fields[14]);
                ps.setString(16, fields[15]);
                ps.setString(17, fields[16]);
                ps.setInt(18, Integer.parseInt(fields[17]));
                ps.setString(19, fields[18]);
                ps.setString(20, fields[19]);

                ps.addBatch();
                batchSize++;

                // Commit mỗi 1000 dòng cho nhanh
                if (batchSize % 1000 == 0) {
                    ps.executeBatch();
                }
            }

            ps.executeBatch();
            conn.commit();

            System.out.println("Insert completed!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
