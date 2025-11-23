package vn.edu.hcmuaf.fit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class LoadDataToFile {

    private static final String DB_URL = "jdbc:mysql://localhost:3307/control";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {
        //Kết nối DB Control
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            //Lấy danh sách Config (fetchConfigs)
            List<Map<String, Object>> configs = fetchConfigs(conn);

            for (Map<String, Object> config : configs) {
                processConfig(conn, config);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Hàm xử lý từng config
    private static void processConfig(Connection conn, Map<String, Object> config) {
        //Lấy thông tin (URL, Tên...)
        int configId = (int) config.get("id");
        String name = (String) config.get("name");
        String url = (String) config.get("url");
        String location = (String) config.get("location_store");

        System.out.println("Đang crawl: " + name + " | URL: " + url);
        boolean success;

        try {
//            Crawl dữ liệu từ url (crawlProducts)
            List<Map<String, String>> products = crawlProducts(url);
//            Lưu file CSV vào folder(saveProductsToCSV)
            String filePath = saveProductsToCSV(location, name, products);
            System.out.println("Đã lưu file: " + filePath);
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        }

        try {
            //    Ghi LOG vào DB (SUCCESS/FAILED)
            writeLog(conn, configId, success);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //Lấy danh sách config từ DB
    private static List<Map<String, Object>> fetchConfigs(Connection conn) throws SQLException {
        List<Map<String, Object>> configs = new ArrayList<>();

        String sql = "SELECT * FROM config WHERE url LIKE 'http%' ";

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> config = new HashMap<>();
                config.put("id", rs.getInt("id"));
                config.put("name", rs.getString("name"));
                config.put("url", rs.getString("url"));
                config.put("location_store", rs.getString("location_store"));
                configs.add(config);
            }
        }
        return configs;
    }

//  Crawl dữ liệu từ url (crawlProducts)
private static List<Map<String, String>> crawlProducts(String url) throws IOException {
    List<Map<String, String>> products = new ArrayList<>();
    Document doc = Jsoup.connect(url)
            .userAgent("Mozilla/5.0")
            .timeout(10000)
            .get();

    // Chỉ lấy các sản phẩm trong div có class "product-list-filter"
    Elements items = doc.select("div.product-list-filter div.product-info");

    for (Element item : items) {
        Map<String, String> p = new HashMap<>();
        p.put("product_name", item.select(".product__name h3").text());
        p.put("category", "Laptop");
        p.put("discount", item.select(".product__price--percent-detail span").text());
        p.put("price_original", item.select(".product__price--through").text());
        p.put("price_sale", item.select(".product__price--show").text());
//        p.put("product_url", item.select(".product__img").attr("src"));
        // ===== LẤY URL SẢN PHẨM CHUẨN =====
        String productUrl = item.select("a.product__link").attr("href");

        // Convert thành absolute URL
        if (productUrl.startsWith("/")) {
            productUrl = "https://cellphones.com.vn" + productUrl;
        }
        System.out.println(productUrl);
        p.put("product_url", productUrl);
        p.put("crawl_date", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        products.add(p);
    }

    System.out.println("Số sản phẩm lấy được: " + products.size());
    return products;
}


    //    Lưu file CSV vào folder(saveProductsToCSV)
    private static String saveProductsToCSV(String location, String name, List<Map<String, String>> products) throws IOException {
        Files.createDirectories(Paths.get(location));
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String filePath = location + File.separator + name + "_" + timestamp + ".csv";

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8))) {

            writer.write("product_name,category,discount,price_original,price_sale,product_url,crawl_date\n");
            for (Map<String, String> p : products) {
                writer.write(String.join(",", Arrays.asList(
                        escapeCSV(p.get("product_name")),
                        escapeCSV(p.get("category")),
                        escapeCSV(p.get("discount")),
                        escapeCSV(p.get("price_original")),
                        escapeCSV(p.get("price_sale")),
                        escapeCSV(p.get("product_url")),
                        escapeCSV(p.get("crawl_date"))
                )));
                writer.newLine();
            }
        }

        return filePath;
    }

    private static String escapeCSV(String value) {
        if (value == null) return "";
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

//    Ghi LOG vào DB (SUCCESS/FAILED)
    private static void writeLog(Connection conn, int configId, boolean success) throws SQLException {
        String sql = "INSERT INTO log (id_config, date_run, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, configId);
            ps.setString(2, success ? "SUCCESS" : "FAILED");
            ps.executeUpdate();
        }
    }
}
