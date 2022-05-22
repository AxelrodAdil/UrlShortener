package com.github.axelrodadil.urlshortener;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class UrlShortenerServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(UrlShortenerServlet.class);
    private static final String PRE_INSERT = "INSERT INTO url_shortener (long_url) VALUES (?)";
    private static final String PRE_GET_LONG_URL = "SELECT long_url FROM url_shortener WHERE url_id = ?";

    private HikariDataSource dataSource;

    private static HikariConfig getHikariConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/url_shortener_db");
        hikariConfig.setUsername("postgres");
        hikariConfig.setPassword("wora");
        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setPoolName("Adil-1");
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setConnectionTimeout(Duration.ofSeconds(30).toMillis());
        hikariConfig.setIdleTimeout(Duration.ofMinutes(2).toMillis());
        return hikariConfig;
    }

    @Override
    public void init() {
        this.dataSource = new HikariDataSource(getHikariConfig());
    }

    @Override
    public void destroy() {
        this.dataSource.close();
    }

    // GET /21432
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String id = req.getParameter("url_id");
        int idNum;
        try {
            logger.info("id-57: " + id);
            idNum = Integer.parseInt(id);
        } catch (NumberFormatException e) {
            logger.error("Invalid id: ", e);
            resp.setStatus(400);
            resp.getWriter().write("Invalid id provided");
            return;
        }
        String longUrl = getLongUrl(idNum);
        if (longUrl == null) {
            resp.setStatus(404);
            resp.getWriter().write("Not found");
            return;
        }
        resp.setStatus(301);
        resp.setHeader("Location", longUrl);
        resp.getWriter().write("Redirect to " + longUrl);
    }

    // POST /
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String longUrl = req.getParameter("url");
        int id = insert(longUrl);
        resp.getWriter().write("ID: " + id);
    }

    private String getLongUrl(int id){
        String longUrl = null;
        try(Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(PRE_GET_LONG_URL)){
            preparedStatement.setInt(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                longUrl = resultSet.getString("long_url");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return longUrl;
    }

    private int insert(String longUrl) {
        int id;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(PRE_INSERT, Statement.RETURN_GENERATED_KEYS)){
            preparedStatement.setString(1, longUrl);
            preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            if(resultSet.next()){
                id = resultSet.getInt("url_id");
            } else {
                throw new RuntimeException("Can not generate new ID");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }
}
