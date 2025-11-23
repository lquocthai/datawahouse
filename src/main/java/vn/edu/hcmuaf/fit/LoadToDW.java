package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;

public class LoadToDW {

    // DB Connections
    private static final String DB_URL_STAGING = "jdbc:mysql://localhost:3307/staging";
    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw";
    private static final String DB_URL_CONTROL = "jdbc:mysql://localhost:3307/control";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    // Tên config trong control.config để log (nếu chưa có, tạo 1 dòng config tương ứng)
    private static final String CONFIG_NAME = "Load To DW";

    public static void main(String[] args) {
        boolean success = false;
        int configId = -1;

        try (Connection connStaging = DriverManager.getConnection(DB_URL_STAGING, DB_USER, DB_PASS);
             Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
             Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {

            // Ensure target tables exist
//            ensureDWTables(connDW);

            // Lấy config id
            configId = getConfigId(connControl, CONFIG_NAME);
            if (configId == -1) {
                System.err.println("Không tìm thấy config với name = '" + CONFIG_NAME + "'. Vui lòng thêm record tương ứng vào control.config");
                // vẫn cho chạy nhưng không log vào control (hoặc có thể dừng)
            }

            // Thực hiện load
            int loaded = loadFactFromTransformed(connStaging, connDW);
            System.out.println("Hoàn tất: đã load " + loaded + " dòng vào DW.");

            success = true;

        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            // Ghi log vào control nếu có configId
            if (configId != -1) {
                try (Connection connControl = DriverManager.getConnection(DB_URL_CONTROL, DB_USER, DB_PASS)) {
                    writeLog(connControl, configId, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Tạo các bảng trong DW nếu chưa có: date_dim và fact_product
     */
    private static void ensureDWTables(Connection conn) throws SQLException {
        String createDateDim =
                "CREATE TABLE IF NOT EXISTS date_dim (\n" +
                        "  date_key INT PRIMARY KEY,\n" +
                        "  full_date DATE NOT NULL,\n" +
                        "  day INT,\n" +
                        "  month INT,\n" +
                        "  quarter INT,\n" +
                        "  year INT,\n" +
                        "  day_name VARCHAR(20),\n" +
                        "  month_name VARCHAR(20),\n" +
                        "  is_weekend BOOLEAN\n" +
                        ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";

        String createFact =
                "CREATE TABLE IF NOT EXISTS fact_product (\n" +
                        "  product_key BIGINT AUTO_INCREMENT PRIMARY KEY,\n" +
                        "  product_name VARCHAR(500),\n" +
                        "  category VARCHAR(255),\n" +
                        "  discount DECIMAL(5,2),\n" +
                        "  price_original DECIMAL(15,2),\n" +
                        "  price_sale DECIMAL(15,2),\n" +
                        "  product_url VARCHAR(1000),\n" +
                        "  date_key INT,\n" +
                        "  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
                        "  FOREIGN KEY (date_key) REFERENCES date_dim(date_key)\n" +
                        ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";

        try (Statement st = conn.createStatement()) {
            st.execute(createDateDim);
            st.execute(createFact);
        }
    }

    /**
     * Load dữ liệu từ staging.transformed_data -> dw.fact_product
     * - Chèn date_dim nếu date_key chưa tồn tại
     * - Batch insert vào fact_product
     */
    private static int loadFactFromTransformed(Connection connStaging, Connection connDW) throws SQLException {
        String selectTransformed = "SELECT product_name, category, discount, price_original, price_sale, product_url, crawl_date FROM transformed_data";

        String insertDateDim = "INSERT IGNORE INTO date_dim (date_key, full_date, day, month, quarter, year, day_name, month_name, is_weekend) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String insertFact = "INSERT INTO fact_product (product_name, category, discount, price_original, price_sale, product_url, date_key) VALUES (?, ?, ?, ?, ?, ?, ?)";

        int totalLoaded = 0;
        connDW.setAutoCommit(false);

        try (Statement st = connStaging.createStatement();
             ResultSet rs = st.executeQuery(selectTransformed);
             PreparedStatement psDate = connDW.prepareStatement(insertDateDim);
             PreparedStatement psFact = connDW.prepareStatement(insertFact)) {

            int batchCount = 0;
            while (rs.next()) {
                String productName = rs.getString("product_name");
                String category = rs.getString("category");
                Double discount = rs.getObject("discount") == null ? 0.0 : rs.getDouble("discount");
                Double priceOriginal = rs.getObject("price_original") == null ? 0.0 : rs.getDouble("price_original");
                Double priceSale = rs.getObject("price_sale") == null ? 0.0 : rs.getDouble("price_sale");
                Date crawlDateSql = rs.getDate("crawl_date"); // DATE (may be null)

                // compute date_key
                int dateKey = 0;
                LocalDate localDate = null;
                if (crawlDateSql != null) {
                    localDate = crawlDateSql.toLocalDate();
                    dateKey = localDate.getYear() * 10000 + localDate.getMonthValue() * 100 + localDate.getDayOfMonth();
                } else {


                }

                // Nếu có date, chèn date_dim (INSERT IGNORE để tránh duplicate)
                if (localDate != null) {
                    psDate.setInt(1, dateKey);
                    psDate.setDate(2, Date.valueOf(localDate));
                    psDate.setInt(3, localDate.getDayOfMonth());
                    psDate.setInt(4, localDate.getMonthValue());
                    int quarter = (localDate.getMonthValue() - 1) / 3 + 1;
                    psDate.setInt(5, quarter);
                    psDate.setInt(6, localDate.getYear());
                    psDate.setString(7, localDate.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
                    psDate.setString(8, localDate.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
                    DayOfWeek dow = localDate.getDayOfWeek();
                    psDate.setBoolean(9, dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY);
                    psDate.addBatch();
                }

                // Prepare fact insert
                psFact.setString(1, productName);
                psFact.setString(2, category);
                if (discount == null) psFact.setNull(3, Types.DECIMAL);
                else psFact.setDouble(3, discount);
                if (priceOriginal == null) psFact.setNull(4, Types.DECIMAL);
                else psFact.setDouble(4, priceOriginal);
                if (priceSale == null) psFact.setNull(5, Types.DECIMAL);
                else psFact.setDouble(5, priceSale);
                psFact.setString(6, rs.getString("product_url"));
                if (localDate != null) psFact.setInt(7, dateKey);
                else psFact.setNull(7, Types.INTEGER);

                psFact.addBatch();

                batchCount++;
                totalLoaded++;

                // Thực thi batch mỗi 1000
                if (batchCount % 1000 == 0) {
                    psDate.executeBatch();
                    psFact.executeBatch();
                    connDW.commit();
                    System.out.println("Đã load " + batchCount + " dòng (tổng: " + totalLoaded + ")");
                    batchCount = 0;
                }
            }

            // execute remaining
            psDate.executeBatch();
            psFact.executeBatch();
            connDW.commit();

        } catch (SQLException e) {
            connDW.rollback();
            throw e;
        } finally {
            connDW.setAutoCommit(true);
        }

        return totalLoaded;
    }

    // Lấy config id từ control.config
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

    // Ghi log vào control.log
    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
