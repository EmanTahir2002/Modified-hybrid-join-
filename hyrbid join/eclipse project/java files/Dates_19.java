package intro;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class Dates_19 {

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/electronica_dw";
        String username = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            populateDateDimension(connection, 2019);
            System.out.println("Date Dimension loaded successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void populateDateDimension(Connection connection, int year) throws SQLException {
        String insertQuery = "INSERT INTO date_dim (DateID, MONTH, YEAR) VALUES (?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, Calendar.JANUARY); 
            calendar.set(Calendar.DAY_OF_MONTH, 1);

            while (calendar.get(Calendar.YEAR) == year) {
                int date = calendar.get(Calendar.DAY_OF_MONTH);

                preparedStatement.setInt(1, date); 
                preparedStatement.setInt(2, calendar.get(Calendar.MONTH) + 1); 
                preparedStatement.setInt(3, calendar.get(Calendar.YEAR));

                preparedStatement.addBatch();
                calendar.add(Calendar.DAY_OF_MONTH, 1);
            }

            preparedStatement.executeBatch();
        }
    }
}
