package vn.edu.hcmuaf.fit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadRawData {
    private static final String DB_URL = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_USER = "user_staging";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {
        // check có tham số hay không
        if (args.length == 0) {
            // In lỗi: Thiếu tên file
            System.err.println("Thiếu tên file CSV. Vui lòng truyền vào tham số dòng lệnh.");
            return;
        }

        String csvFile = args[0];
        String tableName = "raw_data";

        // Kết nối Database Staging
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            // Đọc file CSV vào List
            List<String[]> data = readCSV(csvFile);

            // Kiểm tra dữ liệu rỗng?
            if (data.isEmpty()) {
                // [FLOW: ErrorEmpty] - In lỗi: File trống
                System.err.println("File CSV trống hoặc không có dữ liệu.");
                return;
            }
            // Lấy dòng đầu làm Header (Columns)
            // lấy ra product_name,price,.... vaf xóa nó ra khỏi data luôn
            String[] columns = data.remove(0);
            insertBatch(conn, tableName, columns, data);
        } catch (SQLException e) {
            System.err.println("Lỗi kết nối DB: " + e.getMessage());
        }
    }

    // Hàm đọc CSV
    private static List<String[]> readCSV(String csvFile) {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    String[] values = line.split(",", -1);
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
    private static void insertBatch(Connection conn, String tableName, String[] columns, List<String[]> data) {
        // Tạo câu lệnh INSERT SQL
        String placeholders = String.join(",", Arrays.stream(columns).map(c -> "?").toArray(String[]::new));
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int count = 0;

            // Bắt đầu vòng lặp từng dòng dữ liệu
            for (String[] row : data) {
                // Gán giá trị vào PreparedStatement
                for (int i = 0; i < columns.length; i++) {
                    String value = (row.length > i) ? row[i] : null;
                    pstmt.setString(i + 1, value);
                }

                //pstmt.addBatch
                pstmt.addBatch();

                // kiểm tra Đủ 1000 dòng?
                if (++count % 1000 == 0) {
                    // Execute Batch và in log(Đúng)
                    pstmt.executeBatch();
                    System.out.println(" Đã insert " + count + " dòng...");
                }
                // Quay lại LoopStart nếu còn, xuống dưới nếu hết
            }
            //  Execute Batch phần còn lại (khi hết vòng lặp)
            pstmt.executeBatch();
            // In thông báo hoàn tất
            System.out.println("Hoàn tất insert " + count + " dòng vào bảng " + tableName);

        } catch (SQLException e) {
            System.err.println("Lỗi SQL: " + e.getMessage());
        }
    }
}