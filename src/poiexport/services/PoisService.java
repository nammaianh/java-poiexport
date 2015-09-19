package poiexport.services;

import poiexport.callable.InsertionStatusPrinter;
import vn.itim.engine.indexer.Poiexport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by developer on 12/09/2015.
 */
public class PoisService {

    private Connection connection;

    public PoisService(Connection connection) {
        this.connection = connection;
    }

    public void savePois(List<Poiexport.PoiExport.PoiPb> poiPbs)
            throws SQLException {

        int total = poiPbs.size();
        int doneCount = 0;
        PreparedStatement stmt = null;

        try {
            this.connection.setAutoCommit(true);

            for (Poiexport.PoiExport.PoiPb poi : poiPbs) {

                stmt      = this.buildPoiInsertStatement(poi);
                doneCount += stmt.executeUpdate();
                stmt.close();
                stmt = null;

                stmt = this.buildSideSourceInsertStatement(poi, poi.getSideSources());
                stmt.execute();
                stmt.close();
                stmt = null;

                for (Poiexport.PoiExport.PoiPb.KW kw : poi.getKeywordList()) {

                    stmt = this.buildKeywordInsertStatement(kw, poi);
                    stmt.execute();
                    stmt.close();
                    stmt = null;

                    for (int start : kw.getStartList()) {
                        stmt = this.buildKeywordStartInsertStatement(start, kw);
                        stmt.execute();
                        stmt.close();
                        stmt = null;
                    }
                    for (int end : kw.getEndList()) {
                        stmt = this.buildKeywordEndInsertStatement(end, kw);
                        stmt.execute();
                        stmt.close();
                        stmt = null;
                    }
                    for (long phraseId : kw.getPhraseIdList()) {
                        stmt = this.buildKeywordPhraseIdInsertStatement(phraseId, kw);
                        stmt.execute();
                        stmt.close();
                        stmt = null;
                    }
                }

                for (Poiexport.PoiExport.PoiPb.WorkingTime workingTime : poi.getWorkingTimeList()) {
                    stmt = this.buildWorkingTimeInsertStatement(workingTime, poi);
                    stmt.execute();
                    stmt.close();
                    stmt = null;
                }

                for (Poiexport.PoiExport.PoiPb.MenuItem menuItem : poi.getMenuItemList()) {
                    stmt = this.buildMenuItemInsertStatement(menuItem, poi);
                    stmt.execute();
                    stmt.close();
                    stmt = null;
                }

                for (String titleSynonym : poi.getTitleSynonimsList()) {
                    stmt = this.buildTitleSynonymInsertStatement(titleSynonym, poi);
                    stmt.execute();
                    stmt.close();
                    stmt = null;
                }

                // Print the current percent of done work to the console
                new Thread(new InsertionStatusPrinter(total, doneCount)).run();
            }

//            this.connection.commit();
        }
        catch (Exception ex) {
//            this.connection.rollback();
            throw ex;
        }
        finally {
            if (stmt!=null) stmt.close();
        }
    }

    private PreparedStatement buildTitleSynonymInsertStatement(String titleSynonym, Poiexport.PoiExport.PoiPb poi)
            throws SQLException {

        String query = "INSERT INTO `title_synonyms` VALUE (?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setString(1, titleSynonym);
        stmt.setInt(2, poi.getId());

        return stmt;
    }

    private PreparedStatement buildMenuItemInsertStatement(Poiexport.PoiExport.PoiPb.MenuItem menuItem, Poiexport.PoiExport.PoiPb poi)
            throws SQLException {

        String query = "INSERT INTO `menu_items` VALUE (?,?,?,?,?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setString(1, menuItem.getCategory());
        stmt.setString(2, menuItem.getFoodName());
        stmt.setString(3, menuItem.getFoodNameEn());
        stmt.setDouble(4, menuItem.getPrice());
        stmt.setString(5, menuItem.getDescription());
        stmt.setInt(6, poi.getId());

        return stmt;
    }

    private PreparedStatement buildWorkingTimeInsertStatement(Poiexport.PoiExport.PoiPb.WorkingTime workingTime, Poiexport.PoiExport.PoiPb poi)
            throws SQLException {

        String query = "INSERT INTO `working_times` VALUE (?,?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setInt(1, workingTime.getStart());
        stmt.setInt(2, workingTime.getEnd());
        stmt.setInt(3, poi.getId());

        return stmt;
    }

    private PreparedStatement buildKeywordPhraseIdInsertStatement(long phraseId, Poiexport.PoiExport.PoiPb.KW kw)
            throws SQLException {

        String query = "INSERT INTO `keyword_phrase_ids` VALUE (?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setLong(1, phraseId);
        stmt.setInt(2, kw.getKwId());

        return stmt;
    }

    private PreparedStatement buildKeywordEndInsertStatement(int end, Poiexport.PoiExport.PoiPb.KW kw)
            throws SQLException {

        String query = "INSERT INTO `keyword_ends` VALUE (?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setInt(1, end);
        stmt.setInt(2, kw.getKwId());

        return stmt;
    }

    private PreparedStatement buildKeywordStartInsertStatement(int start, Poiexport.PoiExport.PoiPb.KW kw)
            throws SQLException {

        String query = "INSERT INTO `keyword_starts` VALUE (?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setInt(1, start);
        stmt.setInt(2, kw.getKwId());

        return stmt;
    }

    private PreparedStatement buildKeywordInsertStatement(Poiexport.PoiExport.PoiPb.KW kw, Poiexport.PoiExport.PoiPb poi)
            throws SQLException {
        String query = "INSERT INTO `keywords` VALUE (?,?,?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setInt(1, kw.getKwId());
        stmt.setInt(2, kw.getCnt());
        stmt.setInt(3, kw.getType());
        stmt.setInt(4, poi.getId());

        return stmt;
    }

    private PreparedStatement buildSideSourceInsertStatement(Poiexport.PoiExport.PoiPb poi, Poiexport.PoiExport.PoiPb.SideSource sideSources)
            throws SQLException {

        String query = "INSERT INTO `side_sources` VALUE (?,?,?,?,?,?,?)";
        PreparedStatement stmt = this.connection.prepareStatement(query);

        stmt.setBoolean(1, sideSources.getAgoda());
        stmt.setBoolean(2, sideSources.getFoursquare());
        stmt.setBoolean(3, sideSources.getPlaceVn());
        stmt.setBoolean(4, sideSources.getXemzi());
        stmt.setBoolean(5, sideSources.getEatVn());
        stmt.setBoolean(6, sideSources.getVietnammm());
        stmt.setInt(7, poi.getId());

        return stmt;
    }

    private PreparedStatement buildPoiInsertStatement(Poiexport.PoiExport.PoiPb poi)
            throws SQLException {

        String query = "INSERT INTO `poi_pbs` VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement stmt = this.connection.prepareCall(query);

        stmt.setInt(1, poi.getId());
        stmt.setInt(2, poi.getTypeCode());
        stmt.setDouble(3, poi.getLatitude());
        stmt.setDouble(4, poi.getLongitude());
        stmt.setString(5, poi.getTitle());
        stmt.setString(6, poi.getAddressUnique());
        stmt.setString(7, poi.getAddressGeneral());
        stmt.setString(8, poi.getUrl());
        stmt.setString(9, poi.getPhone());
        stmt.setString(10, poi.getEmail());
        stmt.setString(11, poi.getImg());
        stmt.setBoolean(12, poi.getIsGood());
        stmt.setInt(13, poi.getFeCategory());
        stmt.setInt(14, poi.getGroupId());
        stmt.setInt(15, poi.getTimeUpdated());
        stmt.setInt(16, poi.getSceneId());
        stmt.setInt(17, poi.getFrameStart());
        stmt.setString(18, poi.getRoom());
        stmt.setString(19, poi.getFloor());
        stmt.setString(20, poi.getBuilding());
        stmt.setDouble(21, poi.getRating());
        stmt.setString(22, poi.getDescription());

        return stmt;
    }

}
