package vn.edu.hcmuaf.fit;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class LoadDateDim {

    private static final String DB_URL_DW = "jdbc:mysql://localhost:3307/dw?serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";

    public static void main(String[] args) {

        String insertSQL = "INSERT IGNORE INTO date_dim " +
                "(date_key, full_date, day, month, quarter, year, day_name, month_name, is_weekend, week_of_month, week_of_year) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        LocalDate startDate = LocalDate.of(2020, 1, 1);
        LocalDate endDate = LocalDate.of(2030, 12, 31);

        try (
                Connection connDW = DriverManager.getConnection(DB_URL_DW, DB_USER, DB_PASS);
                PreparedStatement psDW = connDW.prepareStatement(insertSQL)
        ) {
            connDW.setAutoCommit(false);

            WeekFields weekFields = WeekFields.of(Locale.getDefault());
            int batchSize = 0;

            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {

                int dateKey = Integer.parseInt(date.toString().replaceAll("-", ""));
                int weekOfMonth = (date.getDayOfMonth() - 1) / 7 + 1;
                int weekOfYear = date.get(weekFields.weekOfYear());

                psDW.setInt(1, dateKey);
                psDW.setDate(2, Date.valueOf(date));
                psDW.setInt(3, date.getDayOfMonth());
                psDW.setInt(4, date.getMonthValue());
                psDW.setInt(5, (date.getMonthValue() - 1) / 3 + 1); // quarter
                psDW.setInt(6, date.getYear());
                psDW.setString(7, date.getDayOfWeek().name());
                psDW.setString(8, date.getMonth().name());
                psDW.setBoolean(9, date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY);
                psDW.setInt(10, weekOfMonth);
                psDW.setInt(11, weekOfYear);

                psDW.addBatch();
                batchSize++;

                if (batchSize % 1000 == 0) {
                    psDW.executeBatch();
                }
            }

            psDW.executeBatch();
            connDW.commit();

            System.out.println("Date dimension loaded successfully into DW from 2020 to 2030!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
