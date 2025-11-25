package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LoadToDW {

    private static final String DB_URL_STAGING = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";

    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    private static final String CONFIG_NAME = "Load To DW";

    public static void main(String[] args) {
        // 1. Bắt đầu ETL
        boolean success = false;
        int configId = -1;

        // 2. Kết nối tới các database: Staging, DW, Control
        try (Connection connStaging = DriverManager.getConnection(DB_URL_STAGING, DB_USER, DB_PASS);
             Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
             Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {

            // 3. Lấy config id từ bảng control
            configId = getConfigId(connControl, CONFIG_NAME);

            // 4. Kiểm tra config có tồn tại không
            if (configId == -1) {

                // 5. Nếu không tìm thấy config → thông báo lỗi
                System.err.println("⚠ Không tìm thấy config '" + CONFIG_NAME + "' trong control.config.");
                System.err.println("⚠ Vui lòng thêm mới record trước khi chạy log.");
            }

            // 6. Lấy dữ liệu hôm nay từ bảng transformed_data trong staging
            String selectSQL = """
                    SELECT product_name, category, product_url, discount, price_original, price_sale, crawl_date
                    FROM transformed_data 
                    WHERE crawl_date = CURDATE()
                    """;

            try (Statement stmt = connStaging.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSQL)) {

                int count = 0;

                // 7. Lặp qua từng dòng dữ liệu
                while (rs.next()) {

                    // 8. Trích xuất các trường dữ liệu
                    String productName = rs.getString("product_name");
                    String category = rs.getString("category");
                    String productUrl = rs.getString("product_url");
                    double discount = rs.getDouble("discount");
                    double priceOriginal = rs.getDouble("price_original");
                    double priceSale = rs.getDouble("price_sale");
                    String crawlDateStr = rs.getString("crawl_date");

                    // 9. Chuyển crawl_date thành LocalDate và tạo dateKey
                    LocalDate crawlDate = LocalDate.parse(crawlDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    int dateKey = Integer.parseInt(crawlDate.format(DateTimeFormatter.BASIC_ISO_DATE));

                    // 10. Kiểm tra sản phẩm có trong product_dim chưa
                    // 11. Nếu tồn tại trả về productKey, nếu chưa có thì insert mới và trả về productKey
                    long productKey = getOrInsertProduct(connDW, productName, category, productUrl);

                    // 12. Insert hoặc update dữ liệu fact_product
                    insertOrUpdateFact(connDW, productKey, dateKey, discount, priceOriginal, priceSale);

                    // 13. Tăng số lượng dòng đã xử lý
                    count++;
                }

                // 14. In ra số dòng đã load lên DW
                System.out.println("Hoàn tất ETL " + count + " dòng lên DW.");

                success = true;

            }

        } catch (Exception e) {
            // 15. Nếu có lỗi → in stack trace
            e.printStackTrace();
            success = false;
        } finally {
            // 16. Ghi log SUCCESS hoặc FAILED (nếu configId != -1)
            if (configId != -1) {
                try (Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {
                    writeLog(connControl, configId, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 17. Kết thúc ETL

    private static long getOrInsertProduct(Connection conn, String name, String category, String url) throws SQLException {
        String selectSQL = "SELECT product_key FROM product_dim WHERE product_url = ?";
        try (PreparedStatement ps = conn.prepareStatement(selectSQL)) {
            ps.setString(1, url);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong("product_key");
            }
        }

        String insertSQL = "INSERT INTO product_dim (product_name, category, product_url) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.setString(2, category);
            ps.setString(3, url);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        throw new SQLException("Không lấy được product_key cho product: " + url);
    }

    private static void insertOrUpdateFact(Connection conn, long productKey, int dateKey,
                                           double discount, double priceOriginal, double priceSale)
            throws SQLException {

        String sql = """
                INSERT INTO fact_product(product_key, date_key, discount, price_original, price_sale)
                VALUES (?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    discount = VALUES(discount),
                    price_original = VALUES(price_original),
                    price_sale = VALUES(price_sale)
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productKey);
            ps.setInt(2, dateKey);
            ps.setDouble(3, discount);
            ps.setDouble(4, priceOriginal);
            ps.setDouble(5, priceSale);
            ps.executeUpdate();
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

    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
