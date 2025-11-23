package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class testLoadToDW {

    private static final String DB_URL_STAGING = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {

        try (Connection connStaging = DriverManager.getConnection(DB_URL_STAGING, DB_USER, DB_PASS);
             Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS)) {

            String selectSQL = "SELECT product_name, category, product_url, discount, price_original, price_sale, crawl_date " +
                    "FROM transformed_data WHERE crawl_date = CURDATE()";


            try (Statement stmt = connStaging.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSQL)) {

                int count = 0;

                while (rs.next()) {
                    String productName = rs.getString("product_name");
                    String category = rs.getString("category");
                    String productUrl = rs.getString("product_url");
                    double discount = rs.getDouble("discount");
                    double priceOriginal = rs.getDouble("price_original");
                    double priceSale = rs.getDouble("price_sale");
                    String crawlDateStr = rs.getString("crawl_date");

                    LocalDate crawlDate = LocalDate.parse(crawlDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    int dateKey = Integer.parseInt(crawlDate.format(DateTimeFormatter.BASIC_ISO_DATE));

                    // 1. Lấy product_key hoặc insert nếu chưa có
                    long productKey = getOrInsertProduct(connDW, productName, category, productUrl);

                    // 2. Chèn hoặc cập nhật fact_product
                    insertOrUpdateFact(connDW, productKey, dateKey, discount, priceOriginal, priceSale);

                    count++;
                }

                System.out.println("Hoàn tất ETL " + count + " dòng lên DW.");

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

    private static void insertOrUpdateFact(Connection conn, long productKey, int dateKey, double discount, double priceOriginal, double priceSale) throws SQLException {
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
