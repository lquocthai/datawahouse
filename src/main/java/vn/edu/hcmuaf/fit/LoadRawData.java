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
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {

        boolean success = false;
        int configId = -1;

//        1.Có tham số
//        File CSV không?
        if (args.length == 0) {
//            1.1 In lỗi: Thiếu tên file
            System.err.println("Thiếu tên file CSV. Vui lòng truyền tham số.");
            return;
        }

        String csvFile = args[0];
        String tableName = "raw_data";
//        2. Kết nối Database Staging (mysql://localhost:3307/staging), control(mysql://localhost:3307/control)
        try (
                Connection connStaging = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)
        ) {
            // 3. Lấy configId từ control.config có nảm = Load Raw Data
            configId = getConfigId(connControl, "Load Raw Data");
            if (configId == -1) {
                System.err.println("Không tìm thấy config name = 'Load Raw Data'. Kiểm tra control.config");
                return;
            }

            // 4. Xóa data cũ của bảng staging.raw_data
            try (Statement st = connStaging.createStatement()) {
                st.execute("TRUNCATE TABLE raw_data");
                System.out.println("Đã xóa dữ liệu cũ trong raw_data.");
            }

            // 5. Đọc CSV
            List<String[]> data = readCSV(csvFile);
            // 6.kiểm tra có dữ liệu
            if (data.isEmpty()) {
//                6.1 in lỗi: file csv trống
                System.err.println("File CSV trống.");
                return;
            }

            // 6. Lấy header và bỏ dòng đầu
            String[] columns = data.remove(0);

            // 7. Thực hiện insert dữ liệu vào bảng staging.raw_data
            insertBatch(connStaging, tableName, columns, data);

            success = true;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            4.1 ghi log lỗi
//            5.1 ghi log lỗi
//            7.1 ghi log lỗi
            // 8. Ghi log
            if (configId != -1) {
                try (Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {
                    writeLog(connControl, configId, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static int getConfigId(Connection conn, String name) throws SQLException {
        String sql = "SELECT id FROM config WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt("id");
            }
        }
        return -1;
    }
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

        String placeholders = String.join(",", Arrays.stream(columns).map(c -> "?").toArray(String[]::new));
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {

            int count = 0;

            for (String[] row : data) {
                for (int i = 0; i < columns.length; i++) {
                    String value = (row.length > i) ? row[i] : null;
                    pstmt.setString(i + 1, value);
                }

                pstmt.addBatch();

                if (++count % 1000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("Đã insert " + count + " dòng...");
                }
            }

            pstmt.executeBatch();
            System.out.println("Hoàn tất insert " + count + " dòng vào " + tableName);

        } catch (SQLException e) {
            System.err.println("Lỗi SQL: " + e.getMessage());
        }
    }
    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
