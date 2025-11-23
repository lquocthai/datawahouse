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
    private static final String DB_URL_DM = "jdbc:mysql://localhost:3307/datamart?serverTimezone=UTC";
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
                Connection connDM = DriverManager.getConnection(DB_URL_DM, DB_USER, DB_PASS);
                PreparedStatement psDW = connDW.prepareStatement(insertSQL);
                PreparedStatement psDM = connDM.prepareStatement(insertSQL)
        ) {
            connDW.setAutoCommit(false);
            connDM.setAutoCommit(false);

            WeekFields weekFields = WeekFields.of(Locale.getDefault());
            int batchSize = 0;

            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {

                int dateKey = Integer.parseInt(date.toString().replaceAll("-", ""));
                int weekOfMonth = (date.getDayOfMonth() - 1) / 7 + 1;
                int weekOfYear = date.get(weekFields.weekOfYear());

                Object[] values = new Object[]{
                        dateKey,
                        Date.valueOf(date),
                        date.getDayOfMonth(),
                        date.getMonthValue(),
                        (date.getMonthValue() - 1) / 3 + 1, // quarter
                        date.getYear(),
                        date.getDayOfWeek().name(),
                        date.getMonth().name(),
                        date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY,
                        weekOfMonth,
                        weekOfYear
                };

                for (int i = 0; i < values.length; i++) {
                    psDW.setObject(i + 1, values[i]);
                    psDM.setObject(i + 1, values[i]);
                }

                psDW.addBatch();
                psDM.addBatch();
                batchSize++;

                if (batchSize % 1000 == 0) {
                    psDW.executeBatch();
                    psDM.executeBatch();
                }
            }

            psDW.executeBatch();
            psDM.executeBatch();

            connDW.commit();
            connDM.commit();

            System.out.println("Date dimension loaded successfully into DW and DM from 2020 to 2030!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
