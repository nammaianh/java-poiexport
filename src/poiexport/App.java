package poiexport;

import com.google.protobuf.CodedInputStream;
import poiexport.services.CategoryNamesService;
import poiexport.services.LowLevelCategoriesService;
import poiexport.services.PoisService;
import java.io.IOException;
import java.sql.*;
import static vn.itim.engine.indexer.Poiexport.*;

/**
 * Created by developer on 04/09/2015.
 */
public class App {

    private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String DB_LOCATION = "103.48.83.10";
    private final String DB_DATABASE = "locations_db";
    private final String DB_USERNAME = "poiuser";
    private final String DB_PASSWORD = "Changetheworld@123";
    private final String DB_URL      = String.format(
            "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=UTF-8",
            this.DB_LOCATION,
            this.DB_DATABASE
    );

    private Connection connection;
    private CategoryNamesService categoryNamesService;
    private LowLevelCategoriesService lowLevelCategoriesService;
    private PoisService poisService;

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        new App().run();
    }

    private App()
            throws ClassNotFoundException, SQLException {
        this.setupDbConnection();
        this.setupServices();
    }

    private void setupDbConnection()
            throws ClassNotFoundException, SQLException {
        Class.forName(this.JDBC_DRIVER);
        this.connection = DriverManager.getConnection(this.DB_URL, this.DB_USERNAME, this.DB_PASSWORD);
    }

    private void setupServices() {
        this.categoryNamesService = new CategoryNamesService(this.connection);
        this.lowLevelCategoriesService = new LowLevelCategoriesService(this.connection);
        this.poisService = new PoisService(this.connection);
    }

    private void run()
            throws IOException, SQLException {
        String srcDir = "/resources/poiExport";
        CodedInputStream protoDataStream
                = CodedInputStream.newInstance(App.class.getResourceAsStream(srcDir));
        protoDataStream.setSizeLimit(Integer.MAX_VALUE);
        PoiExport poiExport = PoiExport.parseFrom(protoDataStream);

        try {
            this.categoryNamesService.saveCategoryNames(poiExport.getCategoryNamesList());
            this.lowLevelCategoriesService.saveLowLevelCategories(poiExport.getLowLevelCategoriesList());
            this.poisService.savePois(poiExport.getPoiList());
        }
        finally {
            this.connection.close();
        }
    }



}
//0 - ads_categories
//21 - category_names
//803 - low_level_categories
//1053244 - pois