package org.PaymentsAndCommicions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class PaymentProcessor {
    public static Scanner scanner = new Scanner(System.in);
    private static final String SQLITE_CONNECTION_STRING = "jdbc:sqlite:C:/DBs/SQLite/data5.db.db";
    private static final String MONGODB_CONNECTION_STRING = "mongodb+srv://MikolaChe:<PASSWORD>@cluster0.h7nnq4q.mongodb.net/comissions?retryWrites=true&w=majority";
    private static final String MONGODB_DATABASE_NAME = "comissions";
    private static final String MONGODB_COLLECTION_NAME = "comissions";

    public static void main(String[] args) {
        try {
            // Установка соединения с базой SQLite
            Connection sqliteConnection = DriverManager.getConnection(SQLITE_CONNECTION_STRING);

            // Создание таблицы, если она не существует
            createTableIfNotExists(sqliteConnection);

            // Ввод данных по оплате клиента

            String clientName = getInput("Введите ФИО клиента:");
            String agentName = getInput("Введите ФИО агента:");
            int payment = Integer.parseInt(getInput("Введите сумму оплаты клиента:"));

            // Сохранение данных в SQLite
            savePaymentData(sqliteConnection, clientName, agentName, payment);

            // Получение данных из SQLite
            ResultSet resultSet = retrievePaymentData(sqliteConnection);

            // Перенос данных в MongoDB
            transferDataToMongoDB(resultSet);

            // Закрытие соединений
            sqliteConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTableIfNotExists(Connection connection) throws SQLException {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS pays (customer_name TEXT, agent_name TEXT, payment REAL)";
        connection.createStatement().execute(createTableQuery);
    }

    private static String getInput(String message) {
        System.out.println(message);
        return scanner.nextLine();
    }

    private static void savePaymentData(Connection connection, String customer_name, String agent_name, int payment) throws SQLException {
        String insertQuery = "INSERT INTO pays (customer_name, agent_name, payment) VALUES (?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
        preparedStatement.setString(1, customer_name);
        preparedStatement.setString(2, agent_name);
        preparedStatement.setInt(3, payment);
        preparedStatement.executeUpdate();
    }


    private static ResultSet retrievePaymentData(Connection connection) throws SQLException {
        String selectQuery = "SELECT * FROM pays";
        System.out.println(connection.createStatement().executeQuery(selectQuery));
        return connection.createStatement().executeQuery(selectQuery);
    }

    private static void transferDataToMongoDB(ResultSet resultSet) {
        MongoClientURI connectionString = new MongoClientURI(MONGODB_CONNECTION_STRING);
        MongoClient mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(MONGODB_DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(MONGODB_COLLECTION_NAME);

        try {
            while (resultSet.next()) {
                String clientName = resultSet.getString("customer_name");
                String agentName = resultSet.getString("agent_name");
                Integer payment = resultSet.getInt("payment");

                // Создание документа для вставки в коллекцию MongoDB
                Document document = new Document();
                document.append("customer_name", clientName);
                document.append("agent_name", agentName);
                document.append("commission", payment * 0.1);

                // Вставка документа в коллекцию MongoDB
                collection.insertOne(document);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}

