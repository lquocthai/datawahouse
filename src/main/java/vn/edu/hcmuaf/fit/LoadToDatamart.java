package vn.edu.hcmuaf.fit;

import java.sql.*;

public class LoadToDatamart {

    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_URL_DATAMART = "jdbc:mysql://localhost:3307/datamart";
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";

    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    private static final String CONFIG_NAME = "Load To Datamart";

    public static void main(String[] args) {
        boolean success = false;
        int configId = -1;

        try (Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
             Connection connDatamart = DriverManager.getConnection(DB_URL_DATAMART, DB_USER, DB_PASS);
             Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {

            // 1. Tạo bảng nếu chưa có
            ensureDatamartTables(connDatamart);

            // 2. Lấy ID config
            configId = getConfigId(connControl, CONFIG_NAME);
            if (configId == -1) {
                System.err.println("⚠ Không tìm thấy config '" + CONFIG_NAME + "'");
            }

            // 3. Load dữ liệu vào datamart
            int loaded = loadToDatamart(connDW, connDatamart);
            System.out.println("Hoàn tất: đã load " + loaded + " dòng vào dm_product_analysis.");

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

    private static void ensureDatamartTables(Connection conn) throws SQLException {
        String createDM =
                "CREATE TABLE IF NOT EXISTS dm_product_analysis (\n" +
                        " id BIGINT AUTO_INCREMENT PRIMARY KEY,\n" +
                        " product_name VARCHAR(500),\n" +
                        " category VARCHAR(255),\n" +
                        " discount DECIMAL(5,2),\n" +
                        " price_original DECIMAL(15,2),\n" +
                        " price_sale DECIMAL(15,2),\n" +
                        " product_url VARCHAR(1000),\n" +
                        " full_date DATE,\n" +
                        " year INT,\n" +
                        " month INT,\n" +
                        " day INT,\n" +
                        " quarter INT,\n" +
                        " load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
                        ") ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";

        try (Statement st = conn.createStatement()) {
            st.execute(createDM);
        }
    }

    private static int loadToDatamart(Connection connDW, Connection connDM) throws SQLException {

        String sql = """
            SELECT 
                f.product_name, f.category, f.discount, 
                f.price_original, f.price_sale, f.product_url,
                d.full_date, d.year, d.month, d.day, d.quarter
            FROM fact_product f
            LEFT JOIN date_dim d ON f.date_key = d.date_key
        """;

        String insertDM = """
            INSERT INTO dm_product_analysis
            (product_name, category, discount, price_original, price_sale, product_url,
             full_date, year, month, day, quarter)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        int total = 0;
        connDM.setAutoCommit(false);

        try (Statement st = connDW.createStatement();
             ResultSet rs = st.executeQuery(sql);
             PreparedStatement ps = connDM.prepareStatement(insertDM)) {

            while (rs.next()) {
                ps.setString(1, rs.getString("product_name"));
                ps.setString(2, rs.getString("category"));
                ps.setDouble(3, rs.getDouble("discount"));
                ps.setDouble(4, rs.getDouble("price_original"));
                ps.setDouble(5, rs.getDouble("price_sale"));
                ps.setString(6, rs.getString("product_url"));

                ps.setDate(7, rs.getDate("full_date"));
                ps.setObject(8, rs.getObject("year"));
                ps.setObject(9, rs.getObject("month"));
                ps.setObject(10, rs.getObject("day"));
                ps.setObject(11, rs.getObject("quarter"));

                ps.addBatch();
                total++;

                if (total % 1000 == 0) {
                    ps.executeBatch();
                    connDM.commit();
                }
            }

            ps.executeBatch();
            connDM.commit();

        } catch (SQLException e) {
            connDM.rollback();
            throw e;
        } finally {
            connDM.setAutoCommit(true);
        }

        return total;
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
