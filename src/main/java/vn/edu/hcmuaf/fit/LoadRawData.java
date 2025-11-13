package vn.edu.hcmuaf.fit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadRawData {
    // Kết nối database staging
    private static final String DB_URL = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_USER = "user_staging";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Thiếu tên file CSV. Vui lòng truyền vào tham số dòng lệnh.");
            return;
        }

        String csvFile = args[0];
        String tableName = "raw_data";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            // Đọc CSV
            List<String[]> data = readCSV(csvFile);
            if (data.isEmpty()) {
                System.err.println("File CSV trống hoặc không có dữ liệu.");
                return;
            }

            // Lấy header (cột) từ dòng đầu tiên
            String[] columns = data.remove(0);

            //  Batch insert vào database
            insertBatch(conn, tableName, columns, data);

        } catch (SQLException e) {
            System.err.println("Lỗi kết nối DB: " + e.getMessage());
        }
    }

    //Hàm đọc CSV
    private static List<String[]> readCSV(String csvFile) {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    String[] values = line.split(",", -1); // -1 giữ cột trống
                    for (int i = 0; i < values.length; i++) {
                        values[i] = values[i].trim();
                    }
                    rows.add(values);
                }
            }
        } catch (IOException e) {
            System.err.println("Lỗi đọc file CSV: " + e.getMessage());
        }
        return rows;
    }

    //  Hàm batch insert
    private static void insertBatch(Connection conn, String tableName, String[] columns, List<String[]> data) {
        String placeholders = String.join(",", Arrays.stream(columns).map(c -> "?").toArray(String[]::new));
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            int count = 0;

            for (String[] row : data) {
                for (int i = 0; i < columns.length; i++) {
                    String value = (row.length > i) ? row[i] : null;
                    pstmt.setString(i + 1, value);
                }
                pstmt.addBatch();

                if (++count % 1000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("✅ Đã insert " + count + " dòng...");
                }
            }

            pstmt.executeBatch(); // insert phần còn lại
            conn.commit();
            System.out.println("Hoàn tất insert " + count + " dòng vào bảng " + tableName);

        } catch (SQLException e) {
            System.err.println("Lỗi SQL: " + e.getMessage());
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Rollback lỗi: " + ex.getMessage());
            }
        }
    }
}
