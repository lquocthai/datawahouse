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

            // 1. Lấy config ID
            configId = getConfigId(connControl, CONFIG_NAME);
            if (configId == -1) {
                System.err.println("⚠ Không tìm thấy config '" + CONFIG_NAME + "'");
            }

            // 2. Load product_dim từ DW -> DM trước
            loadProductDim(connDW, connDM);

            // 3. Load fact_product
            int factRows = loadFact(connDW, connDM);
            System.out.println("✔ Load fact_product vào DM: " + factRows + " dòng.");

            // 4. Tính toán price_daily
            loadPriceDaily(connDM);

            // 5. Tính toán top_discount_daily
            loadTopDiscountDaily(connDM);

            success = true;

        } catch (Exception e) {
            e.printStackTrace();
            success = false;
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

    // ==============================
    // Load Product Dim
    // ==============================
    private static void loadProductDim(Connection connDW, Connection connDM) throws SQLException {
        String sql = "SELECT product_key, product_name, category, product_url FROM product_dim";
        String insert = """
            INSERT INTO product_dim (product_key, product_name, category, product_url)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                category = VALUES(category),
                product_url = VALUES(product_url)
        """;

        try (Statement st = connDW.createStatement();
             ResultSet rs = st.executeQuery(sql);
             PreparedStatement ps = connDM.prepareStatement(insert)) {

            while (rs.next()) {
                ps.setLong(1, rs.getLong("product_key"));
                ps.setString(2, rs.getString("product_name"));
                ps.setString(3, rs.getString("category"));
                ps.setString(4, rs.getString("product_url"));
                ps.addBatch();
            }
            ps.executeBatch();
        }

        System.out.println("✔ Load product_dim vào DM");
    }

    // ==============================
    // Load Fact Product
    // ==============================
    private static int loadFact(Connection connDW, Connection connDM) throws SQLException {
        String sql = """
            SELECT product_key, date_key, discount, price_original, price_sale
            FROM fact_product
        """;

        String insert = """
            INSERT INTO fact_product (product_key, date_key, discount, price_original, price_sale)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                discount = VALUES(discount),
                price_original = VALUES(price_original),
                price_sale = VALUES(price_sale)
        """;

        int total = 0;

        try (Statement st = connDW.createStatement();
             ResultSet rs = st.executeQuery(sql);
             PreparedStatement ps = connDM.prepareStatement(insert)) {

            while (rs.next()) {
                ps.setLong(1, rs.getLong("product_key"));
                ps.setInt(2, rs.getInt("date_key"));
                ps.setDouble(3, rs.getDouble("discount"));
                ps.setDouble(4, rs.getDouble("price_original"));
                ps.setDouble(5, rs.getDouble("price_sale"));
                ps.addBatch();
                total++;

                if (total % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }

        return total;
    }

    // ==============================
    // Price Daily
    // ==============================
    private static void loadPriceDaily(Connection connDM) throws SQLException {
        String insertAgg = """
            INSERT INTO price_daily (date_key, total_laptops, avg_price_original, avg_price_sale, avg_discount)
            SELECT
                date_key,
                COUNT(*) AS total_laptops,
                AVG(price_original),
                AVG(price_sale),
                AVG(discount)
            FROM fact_product
            GROUP BY date_key
            ON DUPLICATE KEY UPDATE
                total_laptops = VALUES(total_laptops),
                avg_price_original = VALUES(avg_price_original),
                avg_price_sale = VALUES(avg_price_sale),
                avg_discount = VALUES(avg_discount)
        """;

        try (Statement st = connDM.createStatement()) {
            st.execute(insertAgg);
        }

        System.out.println("✔ Cập nhật price_daily");
    }

    // ==============================
    // Top Discount Daily
    // ==============================
    private static void loadTopDiscountDaily(Connection connDM) throws SQLException {
        String deleteOld = """
            DELETE FROM top_discount_daily
            WHERE date_key IN (SELECT DISTINCT date_key FROM fact_product)
        """;

        String insertTop = """
            INSERT INTO top_discount_daily (date_key, product_key, price_sale, discount, rank_in_day)
            SELECT date_key, product_key, price_sale, discount, rank_in_day
            FROM (
                SELECT 
                    date_key, product_key, price_sale, discount,
                    ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY discount DESC) AS rank_in_day
                FROM fact_product
            ) AS ranked
            WHERE rank_in_day <= 10
        """;

        try (Statement st = connDM.createStatement()) {
            st.execute(deleteOld);
            st.execute(insertTop);
        }

        System.out.println("✔ Cập nhật top_discount_daily");
    }

    // ==============================
    // Config ID
    // ==============================
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

    // ==============================
    // Write Log
    // ==============================
    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
