package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TransformData {
    private static final String DB_URL_STAGING = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {
        boolean success = false;
        int configId = -1;

        // 1. Kết nối Database
        try (Connection connStaging = DriverManager.getConnection(DB_URL_STAGING, DB_USER, DB_PASS);
             Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {

            // 1.1 Kết nối thành công
            // 2. Lấy id config theo name
            configId = getConfigId(connControl, "Transform Data");
            // 2.2 Ghi log lỗi
            if (configId == -1) {
                System.err.println("Không tìm thấy config với name = 'Transform Data'. Vui lòng thêm dòng này vào control.config.");
                return;
            }
            // 3. Lấy dữ liệu từ staging.raw_data
            // 4. Tiến hành transform data
            // 5. Load dữ liệu đã transform vào staging.transformed_data
            transform(connStaging);
            success = true;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (configId != -1) {
                try (Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {
                    // 6. Viết log cập nhật trạng thái
                    writeLog(connControl, configId, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 2. Lấy config id theo name
    private static int getConfigId(Connection conn, String name) throws SQLException {
        String sql = "SELECT id FROM config WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt("id");
            }
        }
        return -1; // không tìm thấy
    }
    // Transform Data
    private static void transform(Connection conn) throws SQLException {
        // 3. Lấy dữ liệu từ raw_data
        String selectSQL = "SELECT * FROM raw_data";
        String insertSQL = "INSERT INTO transformed_data " +
                "(product_name, category, discount, price_original, price_sale, product_url, crawl_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            // 4. Tiến hành Transform Data
            // 5. Load dữ liệu đã Transform vào staging.transformed_data
            int count = 0;
            while (rs.next()) {
                pstmt.setString(1, stripQuotes(rs.getString("product_name")));
                pstmt.setString(2, stripQuotes(rs.getString("category")));
                pstmt.setDouble(3, parseDiscount(rs.getString("discount")));
                pstmt.setDouble(4, parsePrice(rs.getString("price_original")));
                pstmt.setDouble(5, parsePrice(rs.getString("price_sale")));
                pstmt.setString(6, stripQuotes(rs.getString("product_url")));
                pstmt.setDate(7, parseDate(rs.getString("crawl_date")));

                pstmt.addBatch();
                count++;
                if (count % 1000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("Đã insert " + count + " dòng...");
                }
            }
            pstmt.executeBatch();
            System.out.println("Hoàn tất transform " + count + " dòng vào transformed_data.");
        }
    }

    private static String stripQuotes(String value) {
        if (value == null) return null;
        return value.replace("\"", "").trim();
    }

    private static double parseDiscount(String value) {
        if (value == null || value.isEmpty()) return 0;
        value = stripQuotes(value).replace("%", "");
        return value.isEmpty() ? 0 : Double.parseDouble(value);
    }

    private static double parsePrice(String value) {
        if (value == null || value.isEmpty()) return 0;
        value = stripQuotes(value).replaceAll("[^0-9]", "");
        return value.isEmpty() ? 0 : Double.parseDouble(value);
    }

    private static Date parseDate(String value) {
        if (value == null || value.isEmpty()) return null;
        value = stripQuotes(value).trim();
        if (value.length() > 10) value = value.substring(0, 10);
        LocalDate d = LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return Date.valueOf(d);
    }
    // 6. Cập nhật trạng thái vào control.log
    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
