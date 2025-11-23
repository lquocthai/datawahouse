package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LoadDWToDM {

    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_URL_DM = "jdbc:mysql://localhost:3307/datamart";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {

        try (Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
             Connection connDM = DriverManager.getConnection(DB_URL_DM, DB_USER, DB_PASS)) {

            // Lấy ngày hiện tại để load
            LocalDate today = LocalDate.now();
            String dateStr = today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // 1. Lấy danh sách sản phẩm từ DW
            String selectProducts = "SELECT product_key, product_name, category, product_url FROM product_dim";
            try (Statement stmtDW = connDW.createStatement();
                 ResultSet rs = stmtDW.executeQuery(selectProducts)) {

                while (rs.next()) {
                    String name = rs.getString("product_name");
                    String category = rs.getString("category");
                    String url = rs.getString("product_url");

                    // Insert vào DM nếu chưa có
                    getOrInsertProduct(connDM, name, category, url);
                }
            }

            // 2. Lấy dữ liệu fact_product của ngày hôm nay từ DW
            String selectFacts = "SELECT f.product_key, f.date_key, f.discount, f.price_original, f.price_sale " +
                    "FROM fact_product f " +
                    "JOIN date_dim d ON f.date_key = d.date_key " +
                    "WHERE d.full_date = ?";
            try (PreparedStatement ps = connDW.prepareStatement(selectFacts)) {
                ps.setString(1, dateStr);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        long productKey = rs.getLong("product_key");
                        int dateKey = rs.getInt("date_key");
                        double discount = rs.getDouble("discount");
                        double priceOriginal = rs.getDouble("price_original");
                        double priceSale = rs.getDouble("price_sale");

                        insertOrUpdateFact(connDM, productKey, dateKey, discount, priceOriginal, priceSale);
                        count++;
                    }
                    System.out.println("Hoàn tất load lên DM: " + count + " dòng fact_product.");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

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
                                           double discount, double priceOriginal, double priceSale) throws SQLException {
        String sql = "INSERT INTO fact_product(product_key, date_key, discount, price_original, price_sale) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "discount = VALUES(discount), price_original = VALUES(price_original), price_sale = VALUES(price_sale)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productKey);
            ps.setInt(2, dateKey);
            ps.setDouble(3, discount);
            ps.setDouble(4, priceOriginal);
            ps.setDouble(5, priceSale);
            ps.executeUpdate();
        }
    }
}
