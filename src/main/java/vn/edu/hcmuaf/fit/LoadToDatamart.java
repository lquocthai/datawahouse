package vn.edu.hcmuaf.fit;

import java.sql.*;

public class LoadToDatamart {

    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_URL_DM = "jdbc:mysql://localhost:3307/datamart";
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";

    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    private static final String CONFIG_NAME = "Load To Datamart";

    public static void main(String[] args) {
        boolean success = false;
        int configId = -1;

        try (Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
             Connection connDM = DriverManager.getConnection(DB_URL_DM, DB_USER, DB_PASS);
             Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {

            configId = getConfigId(connControl, CONFIG_NAME);

            loadPriceDaily(connDW, connDM);
            loadTopDiscountDaily(connDW, connDM);
            loadTop5HighestPrice(connDW, connDM);
            loadTop5LowestPrice(connDW, connDM);

            success = true;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (configId != -1) {
                try (Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {
                    writeLog(connControl, configId, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // ===============================================
    // 1. Price Daily
    // ===============================================
    private static void loadPriceDaily(Connection connDW, Connection connDM) throws SQLException {
        String sql = """
            SELECT d.full_date,
                   COUNT(*) AS total_laptops,
                   AVG(fp.price_original) AS avg_price_original,
                   AVG(fp.price_sale) AS avg_price_sale,
                   AVG(fp.discount) AS avg_discount
            FROM fact_product fp
            JOIN date_dim d ON fp.date_key = d.date_key
            GROUP BY d.full_date
        """;

        try (Statement stDW = connDW.createStatement();
             ResultSet rs = stDW.executeQuery(sql);
             PreparedStatement psDM = connDM.prepareStatement(
                     "REPLACE INTO price_daily (full_date, total_laptops, avg_price_original, avg_price_sale, avg_discount) VALUES (?, ?, ?, ?, ?)"
             )) {
            while (rs.next()) {
                psDM.setDate(1, rs.getDate("full_date"));
                psDM.setInt(2, rs.getInt("total_laptops"));
                psDM.setBigDecimal(3, rs.getBigDecimal("avg_price_original"));
                psDM.setBigDecimal(4, rs.getBigDecimal("avg_price_sale"));
                psDM.setBigDecimal(5, rs.getBigDecimal("avg_discount"));
                psDM.addBatch();
            }
            psDM.executeBatch();
        }
        System.out.println("✔ price_daily updated");
    }

    // ===============================================
    // 2. Top Discount Daily
    // ===============================================
    private static void loadTopDiscountDaily(Connection connDW, Connection connDM) throws SQLException {
        String sql = """
        SELECT
            d.full_date,
            ranked.product_key,
            p.product_name,
            ranked.price_sale,
            ranked.discount,
            ranked.rank_in_day
        FROM (
            SELECT date_key, product_key, price_sale, discount,
                   ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY discount DESC) AS rank_in_day
            FROM fact_product
        ) ranked
        JOIN product_dim p ON ranked.product_key = p.product_key
        JOIN date_dim d ON ranked.date_key = d.date_key
        WHERE ranked.rank_in_day <= 10
        
        """;

        try (Statement stDW = connDW.createStatement();
             ResultSet rs = stDW.executeQuery(sql);
             PreparedStatement psDM = connDM.prepareStatement(
                     "REPLACE INTO top_discount_daily (full_date, product_key, product_name, price_sale, discount, rank_in_day) VALUES (?, ?, ?, ?, ?, ?)"
             )) {
            while (rs.next()) {
                psDM.setDate(1, rs.getDate("full_date"));
                psDM.setLong(2, rs.getLong("product_key"));
                psDM.setString(3, rs.getString("product_name"));
                psDM.setBigDecimal(4, rs.getBigDecimal("price_sale"));
                psDM.setBigDecimal(5, rs.getBigDecimal("discount"));
                psDM.setInt(6, rs.getInt("rank_in_day"));
                psDM.addBatch();
            }
            psDM.executeBatch();
        }
        System.out.println("✔ top_discount_daily updated");
    }

    // ===============================================
    // 3. Top 5 Highest Price
    // ===============================================
    private static void loadTop5HighestPrice(Connection connDW, Connection connDM) throws SQLException {
        String sql = """
            SELECT
                d.full_date,
                fp.product_key,
                p.product_name,
                p.category,
                fp.price_sale,
                ranked.rank_order
            FROM (
                SELECT date_key, product_key, price_sale,
                       ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY price_sale DESC) AS rank_order
                FROM fact_product
            ) ranked
            JOIN product_dim p ON ranked.product_key = p.product_key
            JOIN date_dim d ON ranked.date_key = d.date_key
            JOIN fact_product fp ON fp.product_key = p.product_key AND fp.date_key = d.date_key
            WHERE ranked.rank_order <= 5
        """;

        try (Statement stDW = connDW.createStatement();
             ResultSet rs = stDW.executeQuery(sql);
             PreparedStatement psDM = connDM.prepareStatement(
                     "INSERT INTO top5_highest_price (full_date, product_key, product_name, category, price_sale, rank_order) " +
                             "VALUES (?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE price_sale=VALUES(price_sale), rank_order=VALUES(rank_order)"
             )) {
            while (rs.next()) {
                psDM.setDate(1, rs.getDate("full_date"));
                psDM.setLong(2, rs.getLong("product_key"));
                psDM.setString(3, rs.getString("product_name"));
                psDM.setString(4, rs.getString("category"));
                psDM.setBigDecimal(5, rs.getBigDecimal("price_sale"));
                psDM.setInt(6, rs.getInt("rank_order"));
                psDM.addBatch();
            }
            psDM.executeBatch();
        }
        System.out.println("✔ top5_highest_price updated");
    }

    // ===============================================
    // 4. Top 5 Lowest Price
    // ===============================================
    private static void loadTop5LowestPrice(Connection connDW, Connection connDM) throws SQLException {
        String sql = """
            SELECT
                d.full_date,
                fp.product_key,
                p.product_name,
                p.category,
                fp.price_sale,
                ranked.rank_order
            FROM (
                SELECT date_key, product_key, price_sale,
                       ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY price_sale ASC) AS rank_order
                FROM fact_product
            ) ranked
            JOIN product_dim p ON ranked.product_key = p.product_key
            JOIN date_dim d ON ranked.date_key = d.date_key
            JOIN fact_product fp ON fp.product_key = p.product_key AND fp.date_key = d.date_key
            WHERE ranked.rank_order <= 5
        """;

        try (Statement stDW = connDW.createStatement();
             ResultSet rs = stDW.executeQuery(sql);
             PreparedStatement psDM = connDM.prepareStatement(
                     "INSERT INTO top5_lowest_price (full_date, product_key, product_name, category, price_sale, rank_order) " +
                             "VALUES (?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE price_sale=VALUES(price_sale), rank_order=VALUES(rank_order)"
             )) {
            while (rs.next()) {
                psDM.setDate(1, rs.getDate("full_date"));
                psDM.setLong(2, rs.getLong("product_key"));
                psDM.setString(3, rs.getString("product_name"));
                psDM.setString(4, rs.getString("category"));
                psDM.setBigDecimal(5, rs.getBigDecimal("price_sale"));
                psDM.setInt(6, rs.getInt("rank_order"));
                psDM.addBatch();
            }
            psDM.executeBatch();
        }
        System.out.println("✔ top5_lowest_price updated");
    }

    // ===============================================
    // Control table
    // ===============================================
    private static int getConfigId(Connection conn, String name) throws SQLException {
        String sql = "SELECT id FROM config WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getInt("id");
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
