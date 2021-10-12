//
// $Author$
// $Date$
// $Rev$
// $Id$
//
package gov.mi.mdch.mcir.patient.dao;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.axis.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.publichealthsoftware.base.dao.DAOSysException;
import org.publichealthsoftware.util.OracleDateSupport;
import org.publichealthsoftware.util.Stopwatch;

import gov.mi.mdch.mcir.ErrorConstants;
import gov.mi.mdch.mcir.base.OracleSession;
import gov.mi.mdch.mcir.core.dao.CoreHelperDAOOracle;
import gov.mi.mdch.mcir.data.BrowseChildVO;
import gov.mi.mdch.mcir.data.ChildDetailExtendedVO;
import gov.mi.mdch.mcir.data.ChildVO;
import gov.mi.mdch.mcir.data.FindChildVO;
import gov.mi.mdch.mcir.data.HighRiskStatusVO;
import gov.mi.mdch.mcir.data.RespPartyDetailVO;
import gov.mi.mdch.mcir.data.SiteChildVO;
import gov.mi.mdch.mcir.data.Status;
import gov.mi.mdch.mcir.data.WicChildInfoVO;
import gov.mi.mdch.mcir.patient.persistence.DupMatchResult;
import gov.mi.mdch.mcir.patient.persistence.Patient;
import gov.mi.mdch.mcir.patient.persistence.PatientEthnicity;
import gov.mi.mdch.mcir.patient.persistence.PatientRace;
import gov.mi.mdch.mcir.patient.persistence.PatientRecord;
import gov.mi.mdch.mcir.patient.persistence.Pregnancy;
import gov.mi.mdch.mcir.person.dao.PersonSearchLogHelperDAOOracle;
import gov.mi.mdch.mcir.util.Support;
import gov.mi.mdch.mcir.wsclient.patient.soap.FindGuardianListType;
import gov.mi.mdch.mcir.wsclient.patient.soap.FindGuardianType;
import gov.mi.mdch.mcir.wsclient.patient.soap.FindPatientType;
import gov.mi.mdch.mcir.wsclient.patient.soap.GeoAddressType;


// Important AUTOGEN Notes
//   The PatientAutogenHelperDAOOracle.java file MUST have the getPatientResults() commented out due to
//   alternative methods to convert dates. The database dates are CHAR(8).
//   Patient objects also have transient attributes for wic clinic and health plans
// 

public class PatientHelperDAOOracle extends PatientAutogenHelperDAOOracle{

  static final Log logger = LogFactory.getLog(PatientHelperDAOOracle.class.getName());

  private final static String DB_FILTER_MPI_SEARCH_RESULTS1 =
    "SELECT ch.child_id AS mcir_id "
  + "FROM child ch "
  + "LEFT OUTER JOIN resp_party rp "
  + "  ON (ch.child_id = rp.child_id "
  + "      AND ch.resp_party_id = rp.resp_party_id) ";
  
  private final static String DB_FILTER_MPI_SEARCH_RESULTS2 =
    "JOIN site_roster sr ON (sr.child_id = ch.child_id) ";
  
  private final static String DB_FILTER_MPI_SEARCH_RESULTS3 =
    "JOIN school_site ss ON (ss.site_id = sr.site_id) ";
  
  private final static String DB_FILTER_MPI_SEARCH_RESULTS4 =
    "JOIN school_district sd ON (sd.school_district_id = ss.school_district_id) ";
    
  private final static String DB_FILTER_MPI_SEARCH_RESULTS5 =
      "WHERE ch.child_id IN(";
  
  private final static String DB_FILTER_MPI_SEARCH_RESULTS6 =
      ") AND ch.opt_out_fl = 'N' "
  + "AND ch.deleted_fl = 'N' "
  + "AND rp.record_status_id = 1 ";
  
  private final static String DB_FILTER_MPI_SEARCH_RESULTS7 =
    "AND ss.building_id = ? ";
      
  private final static String DB_FILTER_MPI_SEARCH_RESULTS8 =
    "AND sd.dcode = ? ";
        
  public static ArrayList<String> dbFilterMpiSearchResults(String childIds, String buildingCode, String districtCode)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    ArrayList<String> results = new ArrayList<String>();
    StringBuffer sb = new StringBuffer();

    try {
    
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      sb.append(DB_FILTER_MPI_SEARCH_RESULTS1); 
      if (!districtCode.equals("")
          || !buildingCode.equals("")) {
        sb.append(DB_FILTER_MPI_SEARCH_RESULTS2);
      }
      if (!districtCode.equals("")
          || !buildingCode.equals("")) {
        sb.append(DB_FILTER_MPI_SEARCH_RESULTS3);
      }
      if (!districtCode.equals("")) {
        sb.append(DB_FILTER_MPI_SEARCH_RESULTS4);
      }
      sb.append(DB_FILTER_MPI_SEARCH_RESULTS5);
      sb.append(childIds);
      sb.append(DB_FILTER_MPI_SEARCH_RESULTS6);
      if (!buildingCode.equals("")) {
        sb.append(DB_FILTER_MPI_SEARCH_RESULTS7);
      }
      if (!districtCode.equals("")) {
        sb.append(DB_FILTER_MPI_SEARCH_RESULTS8);
      }
 
      stmt = con.prepareStatement(sb.toString());
      logger.info(sb);
      int parm = 1;
      if (!buildingCode.equals("")) {
        stmt.setInt(parm++, Integer.parseInt(buildingCode));
      }
      if (!districtCode.equals("")) {
        stmt.setString(parm++, districtCode);
      }

      rs = stmt.executeQuery();
       
      while (rs.next()) {
        results.add(rs.getString(1));  // mcir_id
      }

    } catch (SQLException ex) {
      throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }

  private final static String DB_ALT_NAME_IN_MCIR =
      "select child_id "
    + "FROM child "
    + "where (ALT_SCH_FIRST_NM = UTILITIES.tutl_is_search(?) OR ALT_SCH_LAST_NM = UTILITIES.tutl_is_search(?)) AND ROWNUM <= 1";
  
    public static boolean dbSearchValuesFoundInMcirAltNameFields(String searchFirstName, String searchLastName)
      throws DAOSysException {

      Connection con = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
      
        con = CoreHelperDAOOracle.getDAOInstance().getConnection();
        stmt = con.prepareStatement(DB_ALT_NAME_IN_MCIR);

        stmt.setString(1, searchFirstName);
        stmt.setString(2, searchLastName);

        rs = stmt.executeQuery();
         
        if (rs.next()) {
          return true;
        } else {
          return false;
        }

      } catch (SQLException ex) {
        throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
      } finally {
        CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
        CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
        CoreHelperDAOOracle.getDAOInstance().disConnect(con);
      }
    }

  private final static String DB_FIND_PATIENT_TYPES_BY_CHILD_ID1 =
      "SELECT ch.child_id AS mcir_id,"
    + " ch.birth_dt AS birth_date,"
    + " ch.display_nm AS display_name,"
    + " ch.legal_first_nm AS first_name,"
    + " ch.sex_cd AS gender,"
    + " ch.legal_last_nm AS last_name,"
    + " rp.email_addr_tx AS resp_email,"
    + " rp.first_nm AS resp_first_name,"
    + " rp.lang_cd AS resp_language_code,"
    + " rp.last_nm AS resp_last_name,"
    + " rp.middle_nm AS resp_middle_name,"
    + " rp.notify_fl AS resp_notify_flag,"
    + " rp.suffix_nm AS resp_suffix_name,"
    + " na.city AS resp_city,"
    + " c.name AS resp_country,"
    + " county.name AS resp_county,"
    + " na.geo_lattitude AS resp_geo_latitude,"
    + " na.geo_longitude AS resp_geo_longitude,"
    + " na.postal_code AS resp_postal_code,"
    + " s.name AS resp_state,"
    + " na.street_1 AS resp_street_line1,"
    + " na.street_2 AS resp_street_line2,"
    + " pp.area_cd AS resp_primary_area_code,"
    + " pp.phone_nbr_tx AS resp_primary_phone_number,"
    + " pc.area_cd AS resp_cell_area_code,"
    + " pc.phone_nbr_tx AS resp_cell_phone_number "
    + "FROM child ch "
    + "LEFT OUTER JOIN resp_party rp "
    + "  ON (ch.child_id = rp.child_id "
    + "      AND ch.resp_party_id = rp.resp_party_id) "
    + "LEFT OUTER JOIN new_address na ON rp.address_id = na.address_id "
    + "LEFT OUTER JOIN country c ON na.country_id = c.country_id "
    + "LEFT OUTER JOIN county ON na.county_id = county.county_id "
    + "LEFT OUTER JOIN state S ON na.state_id = s.state_id "
    + "LEFT OUTER JOIN phone pp ON rp.primary_phone_id = pp.phone_id "
    + "LEFT OUTER JOIN phone pc ON rp.secondary_phone_id = pc.phone_id "
    + "WHERE ch.child_id IN(";
    
    private final static String DB_FIND_PATIENT_TYPES_BY_CHILD_ID2 =
    ")";
    
    public static HashMap<String,FindPatientType> dbFindPatientTypesByChildId(String childIds)
      throws DAOSysException {

      Connection con = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      String mcirId = "";
      HashMap<String,FindPatientType> results = new HashMap<String,FindPatientType>();

      try {
      
        con = CoreHelperDAOOracle.getDAOInstance().getConnection();
        stmt = con.prepareStatement(DB_FIND_PATIENT_TYPES_BY_CHILD_ID1 + childIds + DB_FIND_PATIENT_TYPES_BY_CHILD_ID2);
        logger.info(DB_FIND_PATIENT_TYPES_BY_CHILD_ID1 + childIds + DB_FIND_PATIENT_TYPES_BY_CHILD_ID2);

        rs = stmt.executeQuery();
         
        while (rs.next()) {
          FindPatientType findPatientType = new FindPatientType();
          FindGuardianListType findGuardianListType = new FindGuardianListType();
          FindGuardianType findGuardianType = new FindGuardianType();
          
          mcirId = rs.getString(1);
          findPatientType.setMcirId(mcirId);
          findPatientType.setBirthDate(rs.getString(2));
          findPatientType.setDisplayName(rs.getString(3));
          findPatientType.setFirstName(rs.getString(4));
          findPatientType.setGender(rs.getString(5));
          findPatientType.setLastName(rs.getString(6));
          
          findGuardianType.setEmail(rs.getString(7));
          findGuardianType.setFirstName(rs.getString(8));
          findGuardianType.setLanguageCode(rs.getString(9));
          findGuardianType.setLastName(rs.getString(10));
          findGuardianType.setMiddleName(rs.getString(11));
          findGuardianType.setNotifyFlag(rs.getString(12));
          findGuardianType.setSuffixName(rs.getString(13));

          GeoAddressType geoAddressType = new GeoAddressType();
          geoAddressType.setCity(rs.getString(14));
          geoAddressType.setCountry(rs.getString(15));
          geoAddressType.setCounty(rs.getString(16));
          geoAddressType.setGeoLatitude(rs.getString(17));
          geoAddressType.setGeoLongitude(rs.getString(18));
          geoAddressType.setPostalCode(rs.getString(19));
          geoAddressType.setState(rs.getString(20));
          geoAddressType.setStreetLine1(rs.getString(21));
          geoAddressType.setStreetLine2(rs.getString(22));
          findGuardianType.setAddress(geoAddressType);
          findGuardianType.setPrimaryAreaCode(rs.getString(23));
          findGuardianType.setPrimaryPhoneNumber(rs.getString(24));
          findGuardianType.setCellAreaCode(rs.getString(25));
          findGuardianType.setCellPhoneNumber(rs.getString(26));
          findGuardianListType.getGuardianList().add(findGuardianType);
          
          findPatientType.setGuardianList(findGuardianListType);
          results.put(mcirId,findPatientType);
        }

      } catch (SQLException ex) {
        throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
      } finally {
        CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
        CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
        CoreHelperDAOOracle.getDAOInstance().disConnect(con);
      }
      return results;
    }

    private final static String DB_FIND_NOTIFICATIONID_BY_REQUESTSTATUSID_CHILDID =
        "SELECT NOTIFICATION_ID "
      + "FROM NOTIFICATION N "
      + "JOIN NOTIFICATION_JOB NJ ON (NJ.NOTIFICATION_JOB_ID = N.NOTIFICATION_JOB_ID) "
      + "WHERE NJ.REQUEST_STATUS_ID = ? "
      + "AND N.CHILD_ID = ? ";
    
    public static String getNotificationIdbyRequestStatusIdandChildId(String requestStatusId, String childId, Connection con)
        throws DAOSysException {

        String notificationId ="";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
          stmt = con.prepareStatement(DB_FIND_NOTIFICATIONID_BY_REQUESTSTATUSID_CHILDID);

          stmt.setString(1, requestStatusId);
          stmt.setString(2, childId);
          rs = stmt.executeQuery();
           
          if (rs.next()) {
            notificationId = rs.getString(1);
          }

        } catch (SQLException ex) {
          throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
        } finally {
          CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
          CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
        }
        return notificationId;
      }

    private final static String DB_FIND_CHILD_RESP_PARTY_DETAILS_BY_REQUESTSTATUSID =
        "select c.child_id,c.legal_first_nm, c.legal_last_nm, to_char(to_date(c.birth_dt,'yyyymmdd'),'mm/dd/yyyy') birth_date, "
      + " r.first_nm,r.last_nm,r.middle_nm,r.lang_cd,r.email_addr_tx, cr.patient_id, "
      + " a.street_1 ||' '|| a.street_2 street ,a.city,st.name,a.postal_code, "
      + " '('|| p.area_cd|| ') '|| substr (p.phone_nbr_tx, 1, 3)|| '-'|| substr (p.phone_nbr_tx, 4)  primary_phone, "
      + " '(' || s.area_cd || ') ' || substr (s.phone_nbr_tx, 1, 3) || '-' || substr (s.phone_nbr_tx, 4)  secondary_phone "
      + " from child c "
      + " join child_request cr on (c.child_id = cr.child_id and cr.status_cd = 1) "
      + " join resp_party r  on (r.resp_party_id = c.resp_party_id) "
      + " join new_address a on (r.address_id = a.address_id) "
      + " join state st  on (a.p_state_id = st.state_id and st.state_id > 0) "
      + " join country ct  on (a.p_country_id = ct.country_id and ct.country_id = 232) "
      + " left outer join phone p on (r.primary_phone_id = p.phone_id) "
      + " left outer join phone s on (r.secondary_phone_id = s.phone_id) "
      + " where cr.request_status_id= ? ";

    public static List<RespPartyDetailVO> getRemRecalExtractCSVReportDataByRequestStatusId(String requestStatusId, Connection con)
        throws DAOSysException {

  	    List<RespPartyDetailVO> results = new ArrayList<RespPartyDetailVO>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        StringBuffer sb = new StringBuffer();
        try {
          stmt = con.prepareStatement(DB_FIND_CHILD_RESP_PARTY_DETAILS_BY_REQUESTSTATUSID);

          stmt.setString(1, requestStatusId);
          sb.append(DB_FIND_CHILD_RESP_PARTY_DETAILS_BY_REQUESTSTATUSID);
          sb.append("Params: 1: "+requestStatusId);
          
          logger.info(sb);
          rs = stmt.executeQuery();
           
         while (rs.next()) {
        	 
              RespPartyDetailVO rp = new RespPartyDetailVO();
              rp.setChildId(rs.getLong(1));
              rp.setSuffixName(rs.getString(2));
              rp.setNotifyCode(rs.getString(3));
              rp.setBirthDate(rs.getString(4));
              rp.setFirstName(rs.getString(5));
              rp.setLastName(rs.getString(6));
              rp.setMiddleName(rs.getString(7));
              rp.setLanguageCode(rs.getString(8));
              rp.setMailQualityCode(rs.getString(9));               
              rp.setPatientRecordNbr(rs.getString(10));
              rp.setAddress(rs.getString(11));
              rp.setCity(rs.getString(12));  
              rp.setStateCode(rs.getString(13));
              rp.setZipRaw(rs.getString(14));
              rp.setPhoneRaw(rs.getString(15));
              rp.setPhoneQualityCode(rs.getString(16));
              results.add(rp);
          }

        } catch (SQLException ex) {
          throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
        } finally {
          CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
          CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
        }
        return results;
      }
       
  public static List<Patient> dbFindInvalidGradeChildListBySiteIdReportPeriodId(Long siteId, Long reportPeriodId, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<Patient> results = new ArrayList<Patient>();

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(Patient.SQL_FIND_INVALID_GRADE_BY_SITE_ID_REPORT_PERIOD_ID);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, siteId);
      stmt.setLong(2, siteId);
      stmt.setLong(3, reportPeriodId);

      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(siteId);
      sb.append(" 2-").append(siteId);
      sb.append(" 3-").append(reportPeriodId);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        Patient obj = PatientHelperDAOOracle.getPatientResults(rs);
        results.add(obj);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }

  public static List<Patient> dbFindMvhaEligibilePatientListBySiteId(Long siteId, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<Patient> results = new ArrayList<Patient>();

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(Patient.SQL_FIND_CHILD).append(Patient.SQL_FILTER_MVHA_ELIGIBLE_PATIENTS_BY_SITE_ID);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, siteId);

      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(siteId);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        Patient obj = PatientHelperDAOOracle.getPatientResults(rs);
        results.add(obj);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }

  public static List<Patient> dbFindVaccineDedupPatientList(Long regionId, Long countyHealthJurisId, Date fromDate, Date toDate, int maxRosterSize, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<Patient> results = new ArrayList<Patient>();

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(Patient.SQL_FIND_CHILD).append(Patient.SQL_FILTER_VACCINE_DEDUP_ROSTER);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, regionId);
      if(countyHealthJurisId == null)
        stmt.setNull(2, Types.NUMERIC);
      else 
        stmt.setLong(2, countyHealthJurisId);
      stmt.setTimestamp(3, OracleDateSupport.getTimestampFromDate(fromDate));
      stmt.setTimestamp(4, OracleDateSupport.getTimestampFromDate(toDate));
      stmt.setLong(5, maxRosterSize);

      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(regionId);
      sb.append(" 2-").append(countyHealthJurisId);
      sb.append(" 3-").append(OracleDateSupport.getOracleDtFromTimestamp(fromDate));
      sb.append(" 4-").append(OracleDateSupport.getOracleDtFromTimestamp(toDate));
      sb.append(" 5-").append(maxRosterSize);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        Patient obj = PatientHelperDAOOracle.getPatientResults(rs);
        results.add(obj);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }

  private final static String DB_REMOVE_PATIENT =
    "{call child_information_processing.P_DELETE_CHILD(" 
  + " pi_session_id => ?"
  + ",pi_child_id => ?"
  + ",po_status_cd => ?"
  + ",po_status_mg => ?"
  + " )}";

  public static Long dbRemovePatient(Patient obj, OracleSession session, Connection con)
    throws DAOSysException {

    long po_status_cd = -1L;
    String po_status_mg = "default";
    String po_result_str = "no results";
    String pi_session_id = session.getLSessionId();
    Long pi_patient_id = obj.getPatientId();

    CallableStatement cstmt = null;

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));

      // check input here and throw exceptions if invalid
      cstmt = con.prepareCall(DB_REMOVE_PATIENT);
      logger.info(DB_REMOVE_PATIENT);

      StringBuffer sb = new StringBuffer();
      sb.append("Parms: ");
      sb.append(" 1-").append(pi_session_id);
      sb.append(" 2-").append(pi_patient_id);
      logger.info(sb.toString());

      //  register OUT params here
      cstmt.setString(1, pi_session_id);
      cstmt.setLong(2, pi_patient_id);
      cstmt.registerOutParameter(3, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);

      cstmt.execute();

      po_status_cd = cstmt.getLong(3);
      po_status_mg = cstmt.getString(4);

      if (po_status_cd < 0) {
        logger.fatal("cd:mg: " + po_status_cd + ":" + po_status_mg);
        CoreHelperDAOOracle.getDAOInstance().processException("PLSQLException", po_status_cd, po_status_mg, po_result_str, null);
      } else {
        po_status_cd = pi_patient_id;
        logger.info("Removed patient: " + po_status_cd);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      CoreHelperDAOOracle.getDAOInstance().processException("SQLException", po_status_cd, po_status_mg, po_result_str, e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
    }
    return Long.valueOf(po_status_cd);
  }

  public static Long dbPostPatientRecord(PatientRecord patientRecord, boolean remove, OracleSession session, Connection con)
    throws DAOSysException {

    long po_status_cd = -1L;
    String pi_session_id = session.getLSessionId();

    CallableStatement cstmt = null;
    
    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));

      ChildDetailExtendedVO child = patientRecord.getChildDetailExtendedVO();
      RespPartyDetailVO respParty = patientRecord.getRespPartyDetailVO();
      SiteChildVO siteChild = patientRecord.getSiteChildVO();
      String highRiskDelimitedList = HighRiskStatusVO.getDelimitedStringList(patientRecord.getHighRiskStatusVOList());
      
      // check input here and throw exceptions if invalid
      if (child != null && child.getId().equals("0")) {
        // add child
        cstmt = con.prepareCall(PatientRecord.P_POST_ADD_CHILD);
        logger.info(PatientRecord.P_POST_ADD_CHILD);

        // Register OUT parameters
        cstmt.registerOutParameter(35, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(36, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(37, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(38, java.sql.Types.VARCHAR);
  
        cstmt.setString(1, pi_session_id);
        cstmt.setString(2, child.getLastName());
        cstmt.setString(3, child.getFirstName());
        cstmt.setString(4, child.getMiddleName());
        cstmt.setString(5, child.getSuffixName());
        cstmt.setString(6, child.getAliasLastName());
        cstmt.setString(7, child.getAliasFirstName());
        cstmt.setString(8, child.getMothersMaidenName());
        cstmt.setString(9, siteChild.getPatientId());
        if(child.getMedicaidId().length() > 0) {
          cstmt.setLong(10, Long.parseLong(child.getMedicaidId()));
        } else {
          cstmt.setNull(10, Types.NUMERIC);
        }
        cstmt.setString(11, child.getWicId());
        cstmt.setString(12, child.getBirthDateRaw());
        cstmt.setString(13, child.getGender());
        cstmt.setString(14, child.getBirthQuantity());
        if(child.getBirthOrder().length() > 0) {
          cstmt.setInt(15, Integer.parseInt(child.getBirthOrder()));
        } else {
          cstmt.setNull(15, Types.NUMERIC);
        }
        cstmt.setString(16, respParty.getStateCode());
        // This check is required for adding out of state children
        if(respParty.getCountyHealthJurisId().length() > 0) {
          cstmt.setInt(17, Integer.parseInt(respParty.getCountyHealthJurisId()));
        } else {
          cstmt.setNull(17, Types.NUMERIC);
        }
        cstmt.setString(18, child.getFacilityName());
        cstmt.setString(19, child.getFacilityStateCode());
        cstmt.setString(20, child.getFacilityCountyCode());
        cstmt.setString(21, respParty.getLastName());
        cstmt.setString(22, respParty.getFirstName());
        cstmt.setString(23, respParty.getMiddleName());
        cstmt.setString(24, respParty.getSuffixName());
        cstmt.setString(25, respParty.getAddress());
        cstmt.setString(26, respParty.getCity());
        cstmt.setString(27, respParty.getStateCode());
        cstmt.setString(28, respParty.getCountryCode());
        cstmt.setString(29, respParty.getZipRaw());
        cstmt.setString(30, respParty.getAreaCodeRaw());
        cstmt.setString(31, respParty.getPhoneRaw());
        cstmt.setString(32, respParty.getPhoneExtension());
        cstmt.setString(33, respParty.getNotifyCode());
        cstmt.setString(34, respParty.getLanguageCode());
        cstmt.setNull(35, Types.NUMERIC);
        cstmt.setNull(36, Types.NUMERIC);
        cstmt.setNull(37, Types.NUMERIC);
        cstmt.setNull(38, Types.VARCHAR);
  
        StringBuffer sb = new StringBuffer();
        sb.append("parms:");
        sb.append(" 1-").append(pi_session_id);
        sb.append(" 2-").append(child.getLastName());
        sb.append(" 3-").append(child.getFirstName());
        sb.append(" 4-").append(child.getMiddleName());
        sb.append(" 5-").append(child.getSuffixName());
        sb.append(" 6-").append(child.getAliasLastName());
        sb.append(" 7-").append(child.getAliasFirstName());
        sb.append(" 8-").append(child.getMothersMaidenName());
        sb.append(" 9-").append(siteChild.getPatientId());
        sb.append(" 10-").append(child.getMedicaidId());
        sb.append(" 11-").append(child.getWicId());
        sb.append(" 12-").append(child.getBirthDateRaw());
        sb.append(" 13-").append(child.getGender());
        sb.append(" 14-").append(child.getBirthQuantity());
        sb.append(" 15-").append(child.getBirthOrder());
        sb.append(" 16-").append(respParty.getStateCode());
        sb.append(" 17-").append(respParty.getCountyHealthJurisId());
        sb.append(" 18-").append(child.getFacilityName());
        sb.append(" 19-").append(child.getFacilityStateCode());
        sb.append(" 20-").append(child.getFacilityCountyCode());
        sb.append(" 21-").append(respParty.getLastName());
        sb.append(" 22-").append(respParty.getFirstName());
        sb.append(" 23-").append(respParty.getMiddleName());
        sb.append(" 24-").append(respParty.getSuffixName());
        sb.append(" 25-").append(respParty.getAddress());
        sb.append(" 26-").append(respParty.getCity());
        sb.append(" 27-").append(respParty.getStateCode());
        sb.append(" 28-").append(respParty.getCountryCode());
        sb.append(" 29-").append(respParty.getZipRaw());
        sb.append(" 30-").append(respParty.getAreaCodeRaw());
        sb.append(" 31-").append(respParty.getPhoneRaw());
        sb.append(" 32-").append(respParty.getPhoneExtension());
        sb.append(" 33-").append(respParty.getNotifyCode());
        sb.append(" 34-").append(respParty.getLanguageCode());
        logger.info(sb.toString());
  
        cstmt.execute();
        
        // get results from proc  
//        long childId = cstmt.getLong(35);
        int status_cd = cstmt.getInt(37);
        String status_mg = cstmt.getString(38);

//        results.setMessage(status_mg);
        if (status_cd > 0) {
//          results.setObject(Long.toString(childId));

        } else {
//          results.setCode(status_cd);
//          results.setValid(false);
        }
        
        if (logger.isInfoEnabled()) {
          logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
        }

      } else {
        // update child

        cstmt = con.prepareCall(PatientRecord.P_POST_CHILD);
        logger.info(PatientRecord.P_POST_CHILD);

        // Register OUT parameters
        cstmt.registerOutParameter(52, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(53, java.sql.Types.VARCHAR);
  
        cstmt.setString(1, pi_session_id);
        cstmt.setLong(2, Long.parseLong(child.getId()));
        cstmt.setString(3, child.getLastName());
        cstmt.setString(4, child.getFirstName());
        cstmt.setString(5, child.getMiddleName());
        cstmt.setString(6, child.getSuffixName());
        cstmt.setString(7, child.getAliasLastName());
        cstmt.setString(8, child.getAliasFirstName());
        cstmt.setString(9, child.getMothersMaidenName());
        cstmt.setString(10, child.getBirthDateRaw());
        cstmt.setString(11, child.getGender());
        cstmt.setString(12, child.getBirthQuantity());
        if(child.getBirthOrder().length() > 0) {
          cstmt.setInt(13, Integer.parseInt(child.getBirthOrder()));
        } else {
          cstmt.setNull(13, Types.NUMERIC);
        }
        cstmt.setString(14, child.getStateCode());
        if(child.getCountyHealthJurisdictionId().length() > 0) {
          cstmt.setInt(15, Integer.parseInt(child.getCountyHealthJurisdictionId()));
        } else {
          cstmt.setNull(15, Types.NUMERIC);
        }
        cstmt.setString(16, child.getFacilityName());
        cstmt.setString(17, child.getFacilityStateCode());
        cstmt.setString(18, child.getFacilityCountyCode());
        cstmt.setString(19, siteChild.getPatientId());
        cstmt.setString(20, child.getWicId());
        cstmt.setString(21, siteChild.hasBirthCertificate() ? "Y" : "N" );
        cstmt.setString(22, child.getGradeId());
        cstmt.setString(23, child.getLastPhysicalDateRaw());
        cstmt.setString(24, child.isVisionScreened() ? "Y" : "N");
        cstmt.setString(25, siteChild.isActive() ? "Y" : "N");
        cstmt.setString(26, siteChild.isRoundup() ? "Y" : "N");
        cstmt.setString(27, siteChild.getReportPeriodId());
        if(child.getMedicaidId().length() > 0) {
          cstmt.setLong(28, Long.parseLong(child.getMedicaidId()));
        } else {
          cstmt.setNull(28, Types.NUMERIC);
        }
        cstmt.setNull(29, Types.VARCHAR); // uic
//        cstmt.setString(12, child.getHealthPlanName());
        cstmt.setString(30, child.getProviderFlag());
        cstmt.setString(31, child.getDeathFlag());
        cstmt.setString(32, child.getMigrantFlag());
        cstmt.setString(33, child.getAliasFlag());
        cstmt.setString(34, child.getOptOutFlag());
        cstmt.setString(35, child.isMovedOrGoneElsewhere() ? "Y" : "N");
        cstmt.setString(36, child.getProviderMogeStatusId());
        cstmt.setString(37, child.getRegionMogeStatusId());
        cstmt.setString(38, child.isHearingScreened() ? "Y" : "N");
        cstmt.setString(39, child.isMvhaEligible() ? "Y" : "N");

        // Resp Party Info
        if(respParty != null) {
          cstmt.setLong(40, Long.parseLong(respParty.getId()));
          cstmt.setString(41, respParty.getAddress());
          cstmt.setString(42, respParty.getCity());
          cstmt.setString(43, respParty.getStateCode());
          cstmt.setString(44, respParty.getZipRaw());
          cstmt.setString(45, respParty.getCountryCode());
          cstmt.setString(46, respParty.getAreaCodeRaw());
          cstmt.setString(47, respParty.getPhoneRaw());
          cstmt.setString(48, respParty.getPhoneExtension());
          cstmt.setString(49, respParty.getLanguageCode());
        } else {
          cstmt.setNull(40, Types.NUMERIC);
          cstmt.setNull(41, Types.VARCHAR);
          cstmt.setNull(42, Types.VARCHAR);
          cstmt.setNull(43, Types.VARCHAR);
          cstmt.setNull(44, Types.VARCHAR);
          cstmt.setNull(45, Types.VARCHAR);
          cstmt.setNull(46, Types.VARCHAR);
          cstmt.setNull(47, Types.VARCHAR);
          cstmt.setNull(48, Types.VARCHAR);
          cstmt.setNull(49, Types.VARCHAR);
        }

        cstmt.setString(50, highRiskDelimitedList);
        cstmt.setString(51, child.getBmiOptOutFlag());
  
        StringBuffer sb = new StringBuffer();
        sb.append("parms:");
        sb.append(" 1-").append(pi_session_id);
        sb.append(" 2-").append(child.getId());
        sb.append(" 3-").append(child.getLastName());
        sb.append(" 4-").append(child.getFirstName());
        sb.append(" 5-").append(child.getMiddleName());
        sb.append(" 6-").append(child.getSuffixName());
        sb.append(" 7-").append(child.getAliasLastName());
        sb.append(" 8-").append(child.getAliasFirstName());
        sb.append(" 9-").append(child.getMothersMaidenName());
        sb.append(" 10-").append(child.getBirthDateRaw());
        sb.append(" 11-").append(child.getGender());
        sb.append(" 12-").append(child.getBirthQuantity());
        sb.append(" 13-").append(child.getBirthOrder());
        sb.append(" 14-").append(child.getStateCode());
        sb.append(" 15-").append(child.getCountyHealthJurisdictionId());
        sb.append(" 16-").append(child.getFacilityName());
        sb.append(" 17-").append(child.getFacilityStateCode());
        sb.append(" 18-").append(child.getFacilityCountyCode());
        sb.append(" 19-").append(siteChild.getPatientId());
        sb.append(" 20-").append(child.getWicId());
        sb.append(" 21-").append(siteChild.hasBirthCertificate() ? "Y" : "N");
        sb.append(" 22-").append(child.getGradeId());
        sb.append(" 23-").append(child.getLastPhysicalDateRaw());
        sb.append(" 24-").append(child.isVisionScreened() ? "Y" : "N");
        sb.append(" 25-").append(siteChild.isActive() ? "Y" : "N");
        sb.append(" 26-").append(siteChild.isRoundup() ? "Y" : "N");
        sb.append(" 27-").append(siteChild.getReportingYearId());
        sb.append(" 28-").append(child.getMedicaidId());
//        sb.append(" 10-").append(child.getHealthPlanName());
        sb.append(" 30-").append(child.getProviderFlag());
        sb.append(" 31-").append(child.getDeathFlag());
        sb.append(" 32-").append(child.getMigrantFlag());
        sb.append(" 33-").append(child.getAliasFlag());
        sb.append(" 34-").append(child.getOptOutFlag());
        sb.append(" 35-").append(child.isMovedOrGoneElsewhere() ? "Y" : "N");
        sb.append(" 36-").append(child.getProviderMogeStatusId());
        sb.append(" 37-").append(child.getRegionMogeStatusId());
        sb.append(" 38-").append(child.isHearingScreened() ? "Y" : "N");
        sb.append(" 39-").append(child.isMvhaEligible() ? "Y" : "N");
        if(respParty != null) {
          sb.append(" 40-").append(respParty.getId());
          sb.append(" 41-").append(respParty.getAddress());
          sb.append(" 42-").append(respParty.getCity());
          sb.append(" 43-").append(respParty.getStateCode());
          sb.append(" 44-").append(respParty.getZipRaw());
          sb.append(" 45-").append(respParty.getCountryCode());
          sb.append(" 46-").append(respParty.getAreaCodeRaw());
          sb.append(" 47-").append(respParty.getPhoneRaw());
          sb.append(" 48-").append(respParty.getPhoneExtension());
          sb.append(" 49-").append(respParty.getLanguageCode());
        }
        sb.append(" 50-").append(highRiskDelimitedList);
        sb.append(" 51-").append(child.getBmiOptOutFlag());
        logger.info(sb.toString());
  
        cstmt.execute();
        
        // get results from proc  
        int status_cd = cstmt.getInt(52);
        String status_mg = cstmt.getString(53);
        
//        results.setMessage(status_mg);
        if (status_cd < 1) {

//          results.setCode(status_cd);
//          results.setValid(false);
        }
        
        if (logger.isInfoEnabled()) {
          logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
        }

      }
  
    } catch(Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return Long.valueOf(po_status_cd);

  }

  public static void dbUpdateDupMatchResult(Long patientId, Long countyHealthJurisId, Long childRegionId, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    if (con==null)
      CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
    logger.debug("using con: " + con);

    try {  
      StringBuffer sb = new StringBuffer();
      sb.append("UPDATE dup_match_result ");
      sb.append("SET base_county_health_juris_id = ? ");
      sb.append(",base_child_rgn = ? ");
      sb.append(" WHERE base_child_id = ? ");
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, countyHealthJurisId);
      stmt.setLong(2, childRegionId);
      stmt.setLong(3, patientId);

      logger.info(sb.toString());
      sb.setLength(0);
      sb.append("Parms: 1-").append(countyHealthJurisId);
      sb.append(" 2-").append(childRegionId);
      sb.append(" 3-").append(patientId);
      logger.info(sb.toString());

      stmt.executeUpdate();

      // Close the first statement before creating the next
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);

      sb.setLength(0);
      sb.append("UPDATE dup_match_result ");
      sb.append("SET match_county_health_juris_id = ? ");
      sb.append(",match_child_rgn = ? ");
      sb.append(" WHERE match_child_id = ? ");
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, countyHealthJurisId);
      stmt.setLong(2, childRegionId);
      stmt.setLong(3, patientId);

      logger.info(sb.toString());
      sb.setLength(0);
      sb.append("Parms: 1-").append(countyHealthJurisId);
      sb.append(" 2-").append(childRegionId);
      sb.append(" 3-").append(patientId);
      logger.info(sb.toString());

      stmt.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e); 
      
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new DAOSysException("RuntimeException", e); 

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
  }

//  @Override
  protected static Patient getPatientResults(ResultSet rs) 
   throws SQLException {
    Patient obj = new Patient();
    obj.setPatientId(rs.getLong(1));
    long responsiblePartyId = rs.getLong(2);
    obj.setResponsiblePartyId(responsiblePartyId==0?null:responsiblePartyId);
    obj.setChildEbcId(rs.getString(3));
    long siteId = rs.getLong(4);
    obj.setSiteId(siteId==0?null:siteId);
    long medicalHomeSiteId = rs.getLong(5);
    obj.setMedicalHomeSiteId(medicalHomeSiteId==0?null:medicalHomeSiteId);
    obj.setChildSourceCode(rs.getString(6));
    obj.setOptOutFlag(rs.getString(7));
    obj.setBirthDate(OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(rs.getString(8),"yyyyMMdd")));
    String deathDate = rs.getString(9);
    obj.setDeathDate(deathDate!=null?OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(deathDate,"yyyyMMdd")):null);
    String deathFlagDate = rs.getString(10);
    obj.setDeathFlagDate(deathFlagDate!=null?OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(deathFlagDate,"yyyyMMdd")):null);
    obj.setDeathFlag(rs.getString(11));
    obj.setSchFirstName(rs.getString(12));
    obj.setSchLastName(rs.getString(13));
    obj.setSchMomsMaidenName(rs.getString(14));
    obj.setSdxLastName(rs.getString(15));
    obj.setSdxFirstName(rs.getString(16));
    obj.setLegalSuffixName(rs.getString(17));
    obj.setLegalFirstName(rs.getString(18));
    obj.setLegalLastName(rs.getString(19));
    obj.setLegalMiddleName(rs.getString(20));
    obj.setMomsMaidenName(rs.getString(21));
    obj.setSexCode(rs.getString(22));
    obj.setAlternateNameFlag(rs.getString(23));
    obj.setDisplayName(rs.getString(24));
    obj.setAlternateSuffixName(rs.getString(25));
    obj.setAlternateSchFirstName(rs.getString(26));
    obj.setAlternateSchLastName(rs.getString(27));
    obj.setAlternateFirstName(rs.getString(28));
    obj.setAlternateLastName(rs.getString(29));
    obj.setAlternateMiddleName(rs.getString(30));
    String alternateNameConfirmDate = rs.getString(31);
    obj.setAlternateNameConfirmDate(alternateNameConfirmDate!=null?OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(alternateNameConfirmDate,"yyyyMMdd")):null);
    obj.setAlternateNameSourceCode(rs.getString(32));
    obj.setAlternateSdxFirstName(rs.getString(33));
    obj.setAlternateSdxLastName(rs.getString(34));
    obj.setSsn(rs.getString(35));
    obj.setWicId(rs.getString(36));
    obj.setClinicId(rs.getString(37));
    obj.setMedicaidId(rs.getString(38));
    obj.setMedicaidSourceCode(rs.getString(39));
    obj.setWicFlag(rs.getString(40));
    obj.setChildRegionId(rs.getLong(41));
    obj.setGradeId(rs.getLong(42));
    long clientSupportId = rs.getLong(43);
    obj.setClientSupportId(clientSupportId==0?null:clientSupportId);
    long recordSourceId = rs.getLong(44);
    obj.setRecordSourceId(recordSourceId==0?null:recordSourceId);
    long recordStatusId = rs.getLong(45);
    obj.setRecordStatusId(recordStatusId==0?null:recordStatusId);
    obj.setCountyHealthJurisId(rs.getLong(46));
    long motherId = rs.getLong(47);
    obj.setMotherId(motherId==0?null:motherId);
    obj.setVisionFlag(rs.getString(48));
    obj.setLastPhysicalDate(rs.getTimestamp(49));
    obj.setCountyCode(rs.getString(50));
    obj.setStateCode(rs.getString(51));
    obj.setBirthStateCode(rs.getString(52));
    obj.setBirthCountyCode(rs.getString(53));
    obj.setBirthHospName(rs.getString(54));
    long birthOrderQuantity = rs.getLong(55);
    obj.setBirthOrderQuantity(birthOrderQuantity==0?null:birthOrderQuantity);
    obj.setPluralityCode(rs.getString(56));
    obj.setLeadAreaFlag(rs.getString(57));
    obj.setLeadStatusFlag(rs.getString(58));
    obj.setLeadStatusDate(rs.getTimestamp(59));
    obj.setMigrantFlag(rs.getString(60));
    obj.setMiLiveFlag(rs.getString(61));
    obj.setMiProviderFlag(rs.getString(62));
    long miProviderChangeQuantity = rs.getLong(63);
    obj.setMiProviderChangeQuantity(miProviderChangeQuantity==0?null:miProviderChangeQuantity);
    obj.setMogeFlag(rs.getString(64));
    long shotQuantity = rs.getLong(65);
    obj.setShotQuantity(shotQuantity==0?null:shotQuantity);
    obj.setEthnicCode(rs.getString(66));
    obj.setRaceCode(rs.getString(67));
    obj.setHearingFlag(rs.getString(68));
    obj.setMvhaEligibleFlag(rs.getString(69));
    obj.setMvhaExpireDate(rs.getTimestamp(70));
    obj.setLastRecordDate(OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(rs.getString(71),"yyyyMMdd")));
    obj.setAdultFlag(rs.getString(72));
    obj.setDeletedFlag(rs.getString(73));
    obj.setDateCreated(rs.getTimestamp(74));
    obj.setDateModified(rs.getTimestamp(75));
    obj.setCreatedBy(rs.getString(76));
    obj.setModifiedBy(rs.getString(77));
    obj.setBmiOptOutFlag(rs.getString(78));
    long hospitalId = rs.getLong(79);
    obj.setHospitalId(hospitalId==0?null:hospitalId);
    obj.setProviderMogeStatusId(rs.getLong(80));
    obj.setRegionMogeStatusId(rs.getLong(81));
    obj.setWicClinic(rs.getString(82));
    obj.setHealthPlanName(rs.getString(83));
    obj.setFwcStatusFlag(rs.getString(84));
//    obj.setFwcStatusEndDate(rs.getTimestamp(85));
    String fwcStatusEndDate = rs.getString(85);
    obj.setFwcStatusEndDate(fwcStatusEndDate!=null?OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(fwcStatusEndDate,"yyyyMMdd")):null);
    obj.setFerpaNoConsentFlag(rs.getString(86));
    obj.setHearingStatusId(rs.getLong(87));
    obj.setVisionStatusId(rs.getLong(88));
    obj.setBirthTime(rs.getString(89));
    obj.setVeraCaseNbr(rs.getString(90));
    return obj;
  }

  //#########################################
  //
  //  Pregnancy
  //
  //#########################################

  public static Pregnancy dbFindCurrentPregnancyByPatientId(Long patientId, Connection con) throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    Pregnancy results = null;

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(Pregnancy.SQL_FIND_PREGNANCY).append(Pregnancy.SQL_FILTER_CURRENT);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, patientId);

      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(patientId);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        results = PatientHelperDAOOracle.getPregnancyResults(rs);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }

//####################3
  private static final String DB_ASSESS_CHILD =
    "{? = call ASSESSMENT.F_ASSESS_CHILD("
  + "p_mcir_id=>?"
  + ",p_prot_id=>?"
  + ",p_action_id=>?"
  + ",p_batch_flag=>?"
  + ",p_status_msg=>?)}";
  
  public static void dbAssessChild(BigDecimal childId, BigDecimal protocolId)
    throws DAOSysException {
  
    if (childId == null)
      throw new DAOSysException("NULL Child Id not allowed.");

    if (protocolId == null)
      throw new DAOSysException("NULL Protocol Id not allowed.");
    
    BigDecimal l_status_cd;
    Connection con = null;
    CallableStatement cstmt = null;
    
    try {

      // check input here and throw exceptions if invalid
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();

      cstmt = con.prepareCall(DB_ASSESS_CHILD);

      logger.info(DB_ASSESS_CHILD);
      
      StringBuffer sb = new StringBuffer();
      sb.append("parms:");
      sb.append(" 2-").append(childId);
      sb.append(" 3-").append(protocolId);
      sb.append(" 4-").append("0");
      sb.append(" 5-").append("Y");
      logger.info(sb.toString());
      
      //  register OUT params here
      cstmt.registerOutParameter(1, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(6, java.sql.Types.VARCHAR);
      
      cstmt.setBigDecimal(2, childId);
      cstmt.setBigDecimal(3, protocolId);
      cstmt.setBigDecimal(4, new BigDecimal("0"));
      cstmt.setString(5, "Y");
      cstmt.setNull(6, Types.VARCHAR);
      
      cstmt.execute();

      l_status_cd = cstmt.getBigDecimal(1);
      String l_status_mg = cstmt.getString(6);
      if (l_status_cd.longValue() != 1) {
        logger.debug("failed: status_cd, status_mg: " + l_status_cd + ":" + l_status_mg);
        throw new DAOSysException(l_status_mg); 
      }
      
    } catch(SQLException ex) {
      ex.printStackTrace();
      logger.error("SQLException thrown. " + ex.getMessage());
      throw new DAOSysException(ex.getMessage());
      
    } catch(DAOSysException ex) {
      ex.printStackTrace();
      throw ex;
      
    } catch(Exception ex) {
      ex.printStackTrace();
      logger.error("Exception thrown. " + ex.getMessage());
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
  }

  private static final String DB_DELETE_CHILD =
    "{call child_information_processing.P_DELETE_CHILD(" 
  + " pi_session_id => ?"
  + ",pi_child_id => ?"
  + ",po_status_cd => ?"
  + ",po_status_mg => ?"
  + " )}";

  public static Status dbDeleteChild(BigDecimal sessionId, long childId)
    throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt= con.prepareCall(DB_DELETE_CHILD);
      logger.info(DB_DELETE_CHILD);

      cstmt.registerOutParameter(3, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);

      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, childId);

      StringBuffer sb = new StringBuffer();
      sb.append("parms:");
      sb.append(" 1-").append(sessionId);
      sb.append(" 2-").append(childId);
      logger.info(sb.toString());
      
      cstmt.execute();

      // get results from proc  
      int status_cd = cstmt.getInt(3);
      String status_mg = cstmt.getString(4);

      results.setDbMessage(status_mg);
      results.setConverted(true);
      if (status_cd < 1) {
        results.setCode(status_cd);
        results.setValid(false);
        sb = new StringBuffer();
        sb.append(ErrorConstants.CHILD_DELETE).append(Math.abs(status_cd));
        results.setMessage(sb.toString());
        logger.error(results);
      }
      
      if (logger.isInfoEnabled()) {
        logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
      }

    } catch(Exception ex) {
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;

  }

  private static final String FIND_CHILD_DETAIL_EXTENDED_BY_CHILD_ID =
    "SELECT c.display_nm, c.birth_dt, c.county_health_juris_id, c.cnty_cd, "
  + "hp.health_plan_nm, c.sex_cd, c.medicaid_id, c.wic_id, c.resp_party_id, "
  + "c.birth_order_qy, c.plurality_cd, c.opt_out_fl, c.migrant_fl, c.child_ebc_id, c.child_src_cd, "
  + "c.legal_first_nm, c.legal_middle_nm, c.legal_last_nm, c.legal_suffix_nm, "
  + "c.alt_first_nm, c.alt_middle_nm, c.alt_last_nm, c.alt_suffix_nm, c.alt_nm_fl, c.alt_nm_confirm_dt, c.alt_nm_src_cd,"
  + Patient.SQL_DEATH_FL + ", c.death_fl_dt, c.death_dt, "
  + "c.mi_prov_fl, c.moms_maiden_nm, c.state_cd, "
  + "c.birth_hosp_nm, c.birth_state_cd, DECODE(c.birth_cnty_cd,'84','82',c.birth_cnty_cd), "
  + "c.lead_area_fl, c.lead_status_fl, c.moge_fl, c.provider_moge_status_id, c.region_moge_status_id, "
  + "c.death_dt, c.child_rgn_id, "
  + "c.grade_id, TO_CHAR(c.last_physical_date,'YYYYMMDD'), c.vision_fl, g.sort_order, c.hearing_fl, "
  + "c.mvha_eligible_fl, c.site_id, wc.clinic_nm, c.bmi_opt_out_fl, c.ferpa_no_consent "
  + "FROM child c, health_plan_map hpm, health_plan hp, grade g, wic_clinic wc "
  + "WHERE c.child_id = hpm.child_id(+)  "
  + "AND NVL(c.deleted_fl, 'N') <> 'Y' "
  + "AND hpm.HEALTH_PLAN_ID = hp.HEALTH_PLAN_ID(+) "
  + "AND hpm.active_fl(+) = 'Y' "
  + "AND c.grade_id = g.grade_id "
  + "AND c.clinic_id = wc.wic_clinic_cd(+) "
  + "AND c.child_id = ?";

  public static ChildDetailExtendedVO dbFindChildDetailExtendedByChildId(long childId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    ChildDetailExtendedVO results = new ChildDetailExtendedVO();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt= con.prepareStatement(FIND_CHILD_DETAIL_EXTENDED_BY_CHILD_ID);
      logger.info(FIND_CHILD_DETAIL_EXTENDED_BY_CHILD_ID);

      stmt.setLong(1, childId);
      logger.info("parms 1-" + Long.toString(childId));
      
      rs = stmt.executeQuery();
      
      while(rs.next()) {
        results.setDisplayName(rs.getString(1));
        results.setBirthDateRaw(rs.getString(2));
        results.setCountyHealthJurisdictionId(rs.getString(3));
        results.setCountyCode(rs.getString(4));
        results.setHealthPlanName(rs.getString(5));
        results.setGender(rs.getString(6));
        results.setMedicaidId(rs.getString(7));
        results.setWicId(rs.getString(8));
        results.setResponsiblePartyId(rs.getLong(9));
        results.setBirthOrder(rs.getInt(10));
        results.setBirthQuantity(rs.getInt(11));
        results.setOptOutFlag(rs.getString(12));
        results.setMigrantFlag(rs.getString(13));
        results.setEbcId(rs.getString(14));
        results.setSourceCode(rs.getString(15));
        results.setFirstName(rs.getString(16));
        results.setMiddleName(rs.getString(17));
        results.setLastName(rs.getString(18));
        results.setSuffixName(rs.getString(19));
        results.setAliasFirstName(rs.getString(20));
        results.setAliasMiddleName(rs.getString(21));
        results.setAliasLastName(rs.getString(22));
        results.setAliasSuffixName(rs.getString(23));
        results.setAliasFlag(rs.getString(24));
        results.setAliasConfirmDateRaw(rs.getString(25));
        results.setAliasSourceCode(rs.getString(26));
        results.setDeathFlag(rs.getString(27));
        results.setDeathFlagDateRaw(rs.getString(28));
        results.setDeathDateRaw(rs.getString(29));
        results.setProviderFlag(rs.getString(30));
        results.setMothersMaidenName(rs.getString(31));
        results.setStateCode(rs.getString(32));
        results.setFacilityName(rs.getString(33));
        results.setFacilityStateCode(rs.getString(34));
        results.setFacilityCountyCode(rs.getString(35));
        results.setLeadAreaFlag(rs.getString(36));
        results.setLeadStatusFlag(rs.getString(37));
        results.setMogeFlag(rs.getString(38));
        results.setProviderMogeStatusId(rs.getString(39));
        results.setRegionMogeStatusId(rs.getString(40));
        results.setDeathDateRaw(rs.getString(41));
        results.setRegionId(rs.getInt(42));
        results.setGradeId(rs.getString(43));
        results.setLastPhysicalDateRaw(rs.getString(44));
        results.setVisionFlag(rs.getString(45));
        results.setGradeSortOrder(rs.getInt(46));
        results.setHearingFlag(rs.getString(47));
        results.setMvhaEligibleFlag(rs.getString(48));
        results.setProviderSiteId(rs.getString(49));
        results.setWicClinicName(rs.getString(50));
        results.setBmiOptOutFlag(rs.getString(51));
        results.setFerpaFlag(rs.getString(52));
        results.setId(childId);
        boolean deceased = results.getDeathFlag().equals("Y") 
                        || (results.getDeathFlagDate().length() > 0)
                        || (results.getDeathDate().length() > 0);
        results.setDeceasedFlag(deceased);
        results.setAge(Support.calculateAge(results.getBirthDateRaw()));
      }

    } catch (SQLException ex) {
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }

  private static final String P_POST_FIND_CHILD_BATCH =
    "{CALL MATCHING.FIND_CHILD_BATCH_RPT ( "
  + "p_mcir_id=>? "
  + ",p_doe_uic_id=>? "
  + ",p_last_name=>? "
  + ",p_first_name=>? "
  + ",p_birth_dt=>? "
  + ",p_sex_cd=>? "
  + ",p_mom_maiden_name=>? "
  + ",p_plurality=>? "
  + ",p_birth_order=>? "
  + ",p_patientid=>? "
  + ",p_medicaidid=>? "
  + ",p_wicid=>? "
  + ",p_ssn=>? "
  + ",p_resp_last_name=>? "
  + ",p_resp_first_name=>? "
  + ",p_resp_phone=>? "
  + ",p_resp_ssn=>? "
  + ",p_moms_last_name=>? "
  + ",p_moms_first_name=>? "
  + ",p_moms_ssn=>? "
  + ",p_ebc_key_id=>? "
  + ",p_status_mg=>? "
  + ",p_status_cd=>? "
  + ",p_like_fl=>'N')}";

  public static Long dbFindChildFromSearchInfoBatch(FindChildVO findChild, OracleSession session)
  throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    Long results = -1L;

    WicChildInfoVO child = (WicChildInfoVO)findChild;

    try {

      con = CoreHelperDAOOracle.dbGetConnection(session);
      logger.debug(session);
      CoreHelperDAOOracle.dbInitializeUser(session, con);
      logger.debug("using con :"+ con);

      cstmt = con.prepareCall(P_POST_FIND_CHILD_BATCH);
      if (logger.isInfoEnabled())
        logger.info(P_POST_FIND_CHILD_BATCH);
  
      // Register OUT parameters
      cstmt.registerOutParameter(22, java.sql.Types.VARCHAR);
      cstmt.registerOutParameter(23, java.sql.Types.NUMERIC);

      if (child.getMcirId() == null || child.getMcirId().equals(0L)) {
        cstmt.setNull(1, Types.INTEGER);
      } else {
        cstmt.setLong(1, child.getMcirId());
      }

      cstmt.setNull(2, Types.VARCHAR);
      cstmt.setString(3, child.getLastName());
      cstmt.setString(4, child.getFirstName());

      if (StringUtils.isEmpty(child.getBirthDate())) {
        cstmt.setNull(5, Types.VARCHAR);
      } else {
        cstmt.setString(5, child.getBirthDate().substring(6)+child.getBirthDate().substring(0,2)+child.getBirthDate().substring(3,5));
      }

      cstmt.setString(6, child.getGender());
      cstmt.setNull(7, Types.VARCHAR);
      cstmt.setNull(8, Types.VARCHAR);
      cstmt.setNull(9, Types.VARCHAR);
      cstmt.setNull(10, Types.VARCHAR);
      cstmt.setNull(11, Types.VARCHAR);
      cstmt.setNull(12, Types.VARCHAR);
      cstmt.setNull(13, Types.VARCHAR); // ssn
      cstmt.setNull(14, Types.VARCHAR);
      cstmt.setNull(15, Types.VARCHAR);
      cstmt.setNull(16, Types.VARCHAR);
//      cstmt.setString(14, child.getResponsiblePartyLastName());
//      cstmt.setString(15, child.getResponsiblePartyFirstName());
//      cstmt.setString(16, child.getResponsiblePartyAreaCodeRaw()+child.getResponsiblePartyPhoneRaw());        
      cstmt.setNull(17, Types.VARCHAR); // responsible party ssn
      cstmt.setNull(18, Types.VARCHAR); 
      cstmt.setNull(19, Types.VARCHAR); 
      cstmt.setNull(20, Types.VARCHAR); 
      cstmt.setNull(21, Types.VARCHAR); 
      cstmt.setNull(22, Types.VARCHAR);
      cstmt.setNull(23, Types.NUMERIC);


      StringBuffer sb = new StringBuffer("");
      sb.append(" 1-"+child.getMcirId()+" ");
      sb.append(" 3-"+child.getLastName()+" ");
      sb.append(" 4-"+child.getFirstName()+" ");

      if (StringUtils.isEmpty(child.getBirthDate())) {
        sb.append(" 5-"+" ");
      } else {
        sb.append(" 5-"+ child.getBirthDate().substring(6)+child.getBirthDate().substring(0,2)+child.getBirthDate().substring(3,5)+" ");
      }

      sb.append(" 6-"+child.getGender()+" ");        
      sb.append(" 11-"+ child.getMedicaidId()+" ");
      sb.append(" 12-"+ child.getWicId()+" ");
      sb.append(" 14-"+ child.getResponsiblePartyLastName()+" ");
      sb.append(" 15-"+ child.getResponsiblePartyFirstName()+" ");
      sb.append(" 16-"+child.getResponsiblePartyAreaCodeRaw()+ child.getResponsiblePartyPhoneRaw()+" ");
      if (logger.isInfoEnabled())
        logger.info("Params : "+sb.toString());        

      cstmt.execute();  

      // get results from proc  
      String status_mg = cstmt.getString(22);
      long status_cd = cstmt.getLong(23);   

      if (logger.isInfoEnabled()) {
        logger.info("status code returned : "+status_cd);
        logger.info("status message returned : "+status_mg);
      }

      //if(status_cd > 0)
      results = Long.valueOf(status_cd) ;
      
    } catch(Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }

  private static final String P_POST_FIND_CHILD_LOADER =
      "{CALL MATCHING.FIND_CHILD_LOADER ( "
    + "p_mcir_id=>? "
    + ",p_last_name=>? "
    + ",p_first_name=>? "
    + ",p_birth_dt=>? "
    + ",p_sex_cd=>? "
    + ",p_mom_maiden_name=>? "
    + ",p_plurality=>? "
    + ",p_birth_order=>? "
    + ",p_patientid=>? "
    + ",p_medicaidid=>? "
    + ",p_wicid=>? "
    + ",p_ssn=>? "
    + ",p_resp_last_name=>? "
    + ",p_resp_first_name=>? "
    + ",p_resp_phone=>? "
    + ",p_resp_ssn=>? "
    + ",p_moms_last_name=>? "
    + ",p_moms_first_name=>? "
    + ",p_moms_ssn=>? "
    + ",p_ebc_key_id=>? "
    + ",p_street_tx=>? "
    + ",p_city_tx=>? "
    + ",p_zip_tx=>? "
    + ",p_status_mg=>? "
    + ",p_status_cd=>? "
    + ",p_like_fl=>'N')}";

    public static Long dbFindChildFromSearchInfoLoader(FindChildVO findChild, OracleSession session)
    throws DAOSysException {

      Connection con = null;
      CallableStatement cstmt = null;
      Long results = -1L;

      WicChildInfoVO child = (WicChildInfoVO)findChild;

      try {

        con = CoreHelperDAOOracle.dbGetConnection(session);
        logger.debug(session);
        CoreHelperDAOOracle.dbInitializeUser(session, con);
        logger.debug("using con :"+ con);

        cstmt = con.prepareCall(P_POST_FIND_CHILD_LOADER);
        if (logger.isInfoEnabled())
          logger.info(P_POST_FIND_CHILD_LOADER);
    
        // Register OUT parameters
        cstmt.registerOutParameter(24, java.sql.Types.VARCHAR);
        cstmt.registerOutParameter(25, java.sql.Types.NUMERIC);

        if (child.getMcirId() == null || child.getMcirId().equals(0L)) {
          cstmt.setNull(1, Types.INTEGER); //p_mcir_id
        } else {
          cstmt.setLong(1, child.getMcirId());
        }

        cstmt.setString(2, child.getLastName());
        cstmt.setString(3, child.getFirstName());

        if (StringUtils.isEmpty(child.getBirthDate())) {
          cstmt.setNull(4, Types.VARCHAR);
        } else {
          cstmt.setString(4, child.getBirthDate().substring(6)+child.getBirthDate().substring(0,2)+child.getBirthDate().substring(3,5));
        }

        cstmt.setString(5, child.getGender());
        cstmt.setString(6, child.getMothersMaidenName()); //p_mom_maiden_name
        cstmt.setNull(7, Types.VARCHAR); //p_plurality
        cstmt.setNull(8, Types.VARCHAR); //p_birth_order
        cstmt.setString(9, child.getPatientId()); //p_patientid
        cstmt.setString(10, child.getMedicaidId()); //p_medicaidid
        cstmt.setString(11, child.getWicId()); //p_wicid
        cstmt.setNull(12, Types.VARCHAR); //p_ssn
        cstmt.setString(13, child.getResponsiblePartyLastName()); //p_resp_last_name
        cstmt.setString(14, child.getResponsiblePartyFirstName()); //p_resp_first_name
        cstmt.setString(15, child.getResponsiblePartyPhoneRaw()); //p_resp_phone
        cstmt.setNull(16, Types.VARCHAR); //p_resp_ssn
        cstmt.setNull(17, Types.VARCHAR);  //p_moms_last_name
        cstmt.setString(18, child.getMothersFirstName());  //p_moms_first_name
        cstmt.setNull(19, Types.VARCHAR);  //p_moms_ssn
        cstmt.setNull(20, Types.VARCHAR);  //p_ebc_key_id
        cstmt.setString(21, child.getStreetTx()); //p_street_tx
        cstmt.setString(22, child.getCityTx()); //p_city_tx
        cstmt.setString(23, child.getZipTx()); //p_zip_tx

        
        StringBuffer sb = new StringBuffer("");      
        sb.append(" 1-"+child.getMcirId()+" ");
        sb.append(" 2-"+child.getLastName()+" ");
        sb.append(" 3-"+child.getFirstName()+" ");

        if (StringUtils.isEmpty(child.getBirthDate())) {
          sb.append(" 4-"+" ");
        } else {
          sb.append(" 4-"+ child.getBirthDate().substring(6)+child.getBirthDate().substring(0,2)+child.getBirthDate().substring(3,5)+" ");
        }

        sb.append(" 5-"+child.getGender()+" ");        
        sb.append(" 6-"+child.getMothersMaidenName()+" ");
        sb.append(" 7-"+" ");
        sb.append(" 8-"+" ");
        sb.append(" 9-"+child.getPatientId()+" ");
        sb.append(" 10-"+child.getMedicaidId()+" ");
        sb.append(" 11-"+child.getWicId()+" ");
        sb.append(" 12-"+" ");
        sb.append(" 13-"+child.getResponsiblePartyLastName()+" ");
        sb.append(" 14-"+child.getResponsiblePartyFirstName()+" ");
        sb.append(" 15-"+child.getResponsiblePartyPhoneRaw()+" ");
        sb.append(" 16-"+" ");
        sb.append(" 17-"+" ");
        sb.append(" 18-"+child.getMothersFirstName()+" ");
        sb.append(" 19-"+" ");
        sb.append(" 20-"+" ");
        sb.append(" 21-"+child.getStreetTx()+" ");
        sb.append(" 22-"+child.getCityTx()+" ");
        sb.append(" 23-"+child.getZipTx()+" ");
        if (logger.isInfoEnabled())
          logger.info("Params : "+sb.toString());        

        cstmt.execute();  

        // get results from proc  
        String status_mg = cstmt.getString(24);
        long status_cd = cstmt.getLong(25);   

        if (logger.isInfoEnabled()) {
          logger.info("status code returned : "+status_cd);
          logger.info("status message returned : "+status_mg);
        }

        //if(status_cd > 0)
        results = Long.valueOf(status_cd) ;
        
      } catch(Exception ex) {
        logger.error(ex.getMessage());
        ex.printStackTrace();
        throw new DAOSysException(ex.getMessage());

      } finally {
        CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
        CoreHelperDAOOracle.getDAOInstance().disConnect(con);
      }

      return results;
    }

  private static final String FIND_CHILDREN_FROM_SEARCH_INFO =
      "SELECT child_id, birth_dt, legal_last_nm, legal_first_nm, display_nm, "
    + "sex_cd, resp_last_nm, resp_first_nm "
    + "FROM TABLE (cast (matching.f_get_child_list(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
    + "AS find_child_type_coll))";

//    + "p_user_id=>?, "
//    + "p_session_id=>?, "
//    + "p_mcir_id=>?, "
//    + "p_doe_uic_id=>?,"
//    + "p_last_name=>?, "
//    + "p_first_name=>?, "
//    + "p_middle_name=>?, "
//    + "p_dob=>?, "
//    + "p_sex=>?, "
//    + "p_mom_maiden_name=>?, "
//    + "p_plurality=>?, "
//    + "p_birth_order=>?, "
//    + "p_patientid=>?, "
//    + "p_medicaidid=>?, "
//    + "p_wicid=>?, "
//    + "p_ssn=>?, "
//    + "p_resp_last_name=>?, "
//    + "p_resp_first_name=>?, "
//    + "p_resp_phone=>?, "
//    + "p_resp_ssn=>?, "
//    + "p_dcode=>?, "
//    + "p_building_id=>?, "
//    + "p_site_id=>?, "
//    + "p_fetch_size=>?) "
  

  /**
   * Returns a collection of BrowseChildVO objects for the purpose of displaying on the
   * on the roster page. If the size of the collection exceeds the fetchsize, then the search
   * is considered to be too broad and a message should accompany the result set informing
   * the user of this. The collection is not in any sort order.
   *
   * @param sessionId - The session ID of the user requesting the search
   * @param child - A <code>FindChildVO</code> object containg the information from the FindChild.jsp page
   * @param fetchsize - A limit specifying the maximum size of the Collection.
   * @return Collection of BrowseChildVO objects for a given set of criteria
   * @exception DAOSysException - when SQLExceptions thrown.
   * @see gov.mi.mdch.mcir.data.BrowseChildVO
   */
  public static Collection dbFindChildrenFromSearchInfo(String userId, BigDecimal sessionId, FindChildVO child, int fetchSize, String source, Boolean logPersonSearchEnabled)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    ArrayList results = new ArrayList();
    Stopwatch timer = new Stopwatch();

    timer.start();
    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt= con.prepareStatement(FIND_CHILDREN_FROM_SEARCH_INFO);
      logger.info(FIND_CHILDREN_FROM_SEARCH_INFO);

      stmt.setString(1, userId);
      stmt.setBigDecimal(2, sessionId);
      if (child.getId().length() != 0) {
        stmt.setLong(3, Long.parseLong(child.getId()));
      } else {
        stmt.setNull(3,Types.NUMERIC);
      }
      stmt.setNull(4, Types.VARCHAR); // uic
      stmt.setString(5, child.getLastName());
      stmt.setString(6, child.getFirstName());
      stmt.setString(7, child.getMiddleName());
      stmt.setString(8, child.getBirthDateRaw());
      stmt.setString(9, child.getGender());
      stmt.setString(10, child.getMothersMaidenName());
      stmt.setString(11, child.getBirthQuantity());
      if (child.getBirthOrder().length() != 0) {
        stmt.setInt(12, Integer.parseInt(child.getBirthOrder()));
      } else {
        stmt.setNull(12,Types.NUMERIC);
      }
      stmt.setString(13, child.getPatientId());
      stmt.setString(14, child.getMedicaidId());
      stmt.setString(15, child.getWicId());
      stmt.setNull(16, Types.VARCHAR); // ssn
      stmt.setString(17, child.getResponsiblePartyLastName());
      stmt.setString(18, child.getResponsiblePartyFirstName());
      stmt.setString(19, child.getResponsiblePartyPhoneRaw());
      stmt.setNull(20, Types.VARCHAR); // resp_ssn
      stmt.setString(21, child.getDistrictCode());
      stmt.setString(22, child.getBuildingCode());
      stmt.setNull(23,Types.VARCHAR);   // we do not use the site_id parameter
      stmt.setInt(24, fetchSize);
      
      
      StringBuffer sb = new StringBuffer();
      sb.append(" 1-").append(userId);
      sb.append(" 2-").append(sessionId);
      sb.append(" 3-").append(child.getId());
      sb.append(" 5-").append(child.getLastName());
      sb.append(" 6-").append(child.getFirstName());
      sb.append(" 7-").append(child.getMiddleName());
      sb.append(" 8-").append(child.getBirthDateRaw());
      sb.append(" 9-").append(child.getGender());
      sb.append(" 10-").append(child.getMothersMaidenName());
      sb.append(" 11").append(child.getBirthQuantity());
      sb.append(" 12-").append(child.getBirthOrder());
      sb.append(" 13-").append(child.getPatientId());
      sb.append(" 14-").append(child.getMedicaidId());
      sb.append(" 15-").append(child.getWicId());
      sb.append(" 17-").append(child.getResponsiblePartyLastName());
      sb.append(" 18-").append(child.getResponsiblePartyFirstName());
      sb.append(" 19-").append(child.getResponsiblePartyAreaCodeRaw() + child.getResponsiblePartyPhoneRaw());
      sb.append(" 21-").append(child.getDistrictCode());
      sb.append(" 22-").append(child.getBuildingCode());
      sb.append(" 23-").append("");
      sb.append(" 24-").append(Integer.toString(fetchSize));
      logger.info("parms: " + sb.toString());
      
      rs = stmt.executeQuery();
      
      while(rs.next()) {
        BrowseChildVO ch = new BrowseChildVO();
        ch.setId(rs.getLong(1));
        ch.setBirthDateRaw(rs.getString(2));
        ch.setLastName(rs.getString(3));
        ch.setFirstName(rs.getString(4));
        ch.setDisplayName(rs.getString(5));
        ch.setGender(rs.getString(6));
        ch.setResponsiblePartyFirstName(rs.getString(7));
        ch.setResponsiblePartyLastName(rs.getString(8));
        results.add(ch);
      }

      timer.stop();
      logger.debug("Elapsed time to find record: " + timer.getElapsedTime() + " ms");


      if (logPersonSearchEnabled) {
        PersonSearchLogHelperDAOOracle.log(source, child, results, timer.getElapsedTime(), userId, sessionId, fetchSize, "MCIR", "MCIR", "", "", con);
      }

    } catch (SQLException ex) {
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }
     
  private static final String DB_FIND_PATIENT_FOR_PHB_CASES =
	      "SELECT "
	    + "cr.child_id, "
	    + "c.birth_dt, "
	    + "c.legal_last_nm, "
	    + "c.legal_first_nm, "
	    + "c.display_nm, "
	    + "c.sex_cd "
	    + "FROM case_roster cr, child c "
	    + "WHERE cr.child_id = c.child_id "
        + "AND cr.case_type_id = 2 ";
  
  public static List<BrowseChildVO> findPatientForPhbCaseAssociates(String userId, FindChildVO child, int fetchSize)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<BrowseChildVO> results = new ArrayList<BrowseChildVO>();
    StringBuffer sb = new StringBuffer();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();

      sb.append(DB_FIND_PATIENT_FOR_PHB_CASES);
      
      if (child.getId().length() != 0) 
    	  sb.append(" AND c.child_id = ? ");  
   
      if (child.getFirstName() !=null && !child.getFirstName().isEmpty())
	        sb.append(" AND UPPER(c.legal_first_nm) LIKE ? ");

      if (child.getLastName() !=null && !child.getLastName().isEmpty())
    	  sb.append(" AND UPPER(c.legal_last_nm) LIKE ? ");

      if (child.getBirthDate() != null && !child.getBirthDate().isEmpty()) 
    	  sb.append(" AND c.birth_dt = ? ");

      if (child.getGender() != null && !child.getGender().isEmpty()) 
    	  sb.append(" AND c.sex_cd = ? ");

      stmt = con.prepareStatement(sb.toString());
     
      int idx=1;
      
      if (child.getId().length() != 0) {
        sb.append(" ").append(idx).append("-").append(child.getId());
        stmt.setLong(idx++, Long.valueOf(child.getId()));
      } 
      
      if (child.getLastName() !=null && !child.getLastName().isEmpty()) {
        sb.append(" ").append(idx).append("-").append(child.getLastName());
        stmt.setString(idx++, child.getLastName().replace('*','%').toUpperCase());
      }
 
      if (child.getFirstName() !=null && !child.getFirstName().isEmpty()) {
        sb.append(" ").append(idx).append("-").append(child.getFirstName());
        stmt.setString(idx++, child.getFirstName().replace('*','%').toUpperCase());
      }
  
      if (child.getBirthDate() != null && !child.getBirthDate().isEmpty()) {
        sb.append(" ").append(idx).append("-").append(child.getBirthDateRaw());
        stmt.setString(idx++, child.getBirthDateRaw());
      }
      
      if (child.getGender() != null && !child.getGender().isEmpty()) {
        sb.append(" ").append(idx).append("-").append(child.getGender());
        stmt.setString(idx++, child.getGender());
      }

      logger.info("SQL: " + sb.toString());
      
      rs = stmt.executeQuery();
      
      while(rs.next()) {
        BrowseChildVO ch = new BrowseChildVO();
        ch.setId(rs.getLong(1));
        ch.setBirthDateRaw(rs.getString(2));
        ch.setLastName(rs.getString(3));
        ch.setFirstName(rs.getString(4));
        ch.setDisplayName(rs.getString(5));
        ch.setGender(rs.getString(6));
        results.add(ch);
      }
      
    } catch (SQLException ex) {
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }

  private static final String DB_FIND_MOGE_CHILDREN =
    "SELECT DISTINCT c.child_id, c.display_nm, c.birth_dt "
  + "FROM child c, site_roster sr "
  + "WHERE c.child_id = sr.child_id(+) "
  + "AND NVL(c.deleted_fl, 'N') <> 'Y' "
  + "AND c.moge_fl = 'Y' ";

  public static Collection dbFindMogeChildren(String siteId, boolean byRoster)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    TreeSet results = new TreeSet();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      StringBuffer sb = new StringBuffer();
      sb.append(DB_FIND_MOGE_CHILDREN);
      if(byRoster)
        sb.append("AND sr.site_id = ? ");
      else
        sb.append("AND c.site_id = ? ");
      stmt = con.prepareStatement(sb.toString());
      logger.info(sb.toString());

      stmt.setString(1, siteId);
      logger.info("parms: 1-" + siteId);

      rs = stmt.executeQuery();

      while(rs.next()) {
        ChildVO ch = new ChildVO();
        ch.setId(rs.getLong(1));
        ch.setDisplayName(rs.getString(2));
        ch.setBirthDateRaw(rs.getString(3));
        results.add(ch);
      }

    } catch (Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }
  

  public static final String DB_FIND_NON_MI_PRV_CHILDREN =
    "SELECT DISTINCT c.child_id, c.display_nm, c.birth_dt "
  + "FROM child c, site_roster sr "
  + "WHERE c.child_id = sr.child_id(+) "
  + "AND NVL(c.deleted_fl, 'N') <> 'Y' "
  + "AND NVL(c.opt_out_fl, 'N') <> 'Y' " //Bug# 13474
  + "AND c.mi_prov_fl = 'N' ";

  public static Collection dbFindNonMiPrvChildren(String siteId, boolean byRoster)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    TreeSet results = new TreeSet();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      StringBuffer sb = new StringBuffer();
      sb.append(DB_FIND_NON_MI_PRV_CHILDREN);
      if(byRoster)
        sb.append("AND sr.site_id = ? ");
      else
        sb.append("AND c.site_id = ? ");
      stmt = con.prepareStatement(sb.toString());
      logger.info(sb.toString());

      stmt.setString(1, siteId);
      logger.info("parms: 1-" + siteId);

      rs = stmt.executeQuery();

      while(rs.next()) {
        ChildVO ch = new ChildVO();
        ch.setId(rs.getLong(1));
        ch.setDisplayName(rs.getString(2));
        ch.setBirthDateRaw(rs.getString(3));
        results.add(ch);
      }

    } catch (Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }
  
      
  private static final String DB_RESP_PARTY_BY_CHILD_ID =
      "SELECT "
    + "rp.county_health_juris_id, "
    + "resp_area_cd, "
    + "resp_city_tx, "
    + "resp_cntry_cd, "
    + "resp_confirm_dt, "
    + "resp_first_nm, "
    + "rp.resp_party_id, "
    + "resp_lang_cd, "
    + "resp_last_nm, "
    + "valid_address_fl, "
    + "resp_middle_nm, "
    + "resp_notify_fl, "
    + "resp_phone_tx, "
    + "resp_phone_ex_tx, "
    + "valid_phone_fl, "
    + "resp_state_cd, "
    + "resp_street_tx, "
    + "resp_suffix_nm, "
    + "resp_zip_tx, "
    + "to_char(NVL(rp.date_modified, rp.date_created), 'yyyymmdd'), "
    + "rp.record_status_id, "
    + "c.resp_party_id, "
    + "to_char(rp.last_addr_date, 'MM/dd/yyyy') "
    + "FROM resp_party_new_view rp, child c "
    + "WHERE rp.child_id = c.child_id "
    + "AND NVL(c.deleted_fl, 'N') <> 'Y' "
    + "AND rp.record_status_id = 1 "
    + "AND rp.child_id = ?";

  public static Collection dbFindRespPartyByChildId(long childId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    ArrayList results = new ArrayList();

    try {
    
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt = con.prepareStatement(DB_RESP_PARTY_BY_CHILD_ID);
      logger.info(DB_RESP_PARTY_BY_CHILD_ID);

      stmt.setLong(1, childId);
      logger.info("parms: 1-" + Long.toString(childId));

      rs = stmt.executeQuery();
       
      while (rs.next()) {
        RespPartyDetailVO rp = new RespPartyDetailVO();
        rp.setCountyHealthJurisId(rs.getString(1));
        rp.setAreaCodeRaw(rs.getString(2));
        rp.setCity(rs.getString(3));
        rp.setCountryCode(rs.getString(4));
        rp.setConfirmDateRaw(rs.getString(5));
        rp.setFirstName(rs.getString(6));
        rp.setId(rs.getLong(7));
        rp.setLanguageCode(rs.getString(8));
        rp.setLastName(rs.getString(9));
        rp.setMailQualityCode(rs.getString(10));
        rp.setMiddleName(rs.getString(11));
        rp.setNotifyCode(rs.getString(12));
        rp.setPhoneRaw(rs.getString(13));
        rp.setPhoneExtension(rs.getString(14));
        rp.setPhoneQualityCode(rs.getString(15));
        rp.setStateCode(rs.getString(16));
        rp.setAddress(rs.getString(17));
        rp.setSuffixName(rs.getString(18));
        rp.setZipRaw(rs.getString(19));
        rp.setModifiedDateRaw(rs.getString(20));
        rp.setRecordStatusId(rs.getString(21));
        rp.setChildId(childId);
        String chRpId = rs.getString(22);
        rp.setDefaultFlag(rp.getId().equals(chRpId));
        rp.setAddressModifiedDate(rs.getString(23));
        results.add(rp);
      }

    } catch (SQLException ex) {
      throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
    
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    
    }
    return results;
  }

  private static final String RESP_PARTY_DETAIL_BY_RESP_PARTY_ID =
      "SELECT "
    + "rp.county_health_juris_id, "
    + "resp_area_cd, "
    + "resp_city_tx, "
    + "resp_cntry_cd, "
    + "resp_confirm_dt, "
    + "resp_first_nm, "
    + "rp.child_id, "
    + "resp_lang_cd, "
    + "resp_last_nm, "
    + "valid_address_fl, "
    + "resp_middle_nm, "
    + "resp_notify_fl, "
    + "resp_phone_tx, "
    + "resp_phone_ex_tx, "
    + "valid_phone_fl, "
    + "resp_state_cd, "
    + "resp_street_tx, "
    + "resp_suffix_nm, "
    + "resp_zip_tx, "
    + "to_char(NVL(rp.date_modified, rp.date_created), 'yyyymmdd'), "
    + "rp.record_status_id, "
    + "c.resp_party_id, "
    + "to_char(rp.last_addr_date, 'MM/dd/yyyy') "
    + "FROM resp_party_new_view rp, child c "
    + "WHERE rp.child_id = c.child_id "
    + "AND NVL(c.deleted_fl, 'N') <> 'Y' "
    + "AND rp.resp_party_id = ?";

  public static RespPartyDetailVO dbFindRespPartyDetailByRespPartyId(long respPartyId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    RespPartyDetailVO results = new RespPartyDetailVO();

    try {
    
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt = con.prepareStatement(RESP_PARTY_DETAIL_BY_RESP_PARTY_ID);
      logger.info(RESP_PARTY_DETAIL_BY_RESP_PARTY_ID);

      stmt.setLong(1, respPartyId);
      logger.info("parms: 1-" + Long.toString(respPartyId));

      rs = stmt.executeQuery();
       
      while (rs.next()) {
        results.setCountyHealthJurisId(rs.getString(1));
        results.setAreaCodeRaw(rs.getString(2));
        results.setCity(rs.getString(3));
        results.setCountryCode(rs.getString(4));
        results.setConfirmDateRaw(rs.getString(5));
        results.setFirstName(rs.getString(6));
        results.setChildId(rs.getLong(7));
        results.setLanguageCode(rs.getString(8));
        results.setLastName(rs.getString(9));
        results.setMailQualityCode(rs.getString(10));
        results.setMiddleName(rs.getString(11));
        results.setNotifyCode(rs.getString(12));
        results.setPhoneRaw(rs.getString(13));
        results.setPhoneExtension(rs.getString(14));
        results.setPhoneQualityCode(rs.getString(15));
        results.setStateCode(rs.getString(16));
        results.setAddress(rs.getString(17));
        results.setSuffixName(rs.getString(18));
        results.setZipRaw(rs.getString(19));
        results.setModifiedDateRaw(rs.getString(20));
        results.setRecordStatusId(rs.getString(21));
        results.setId(respPartyId);

        long chRpId = rs.getLong(22);
        results.setDefaultFlag(chRpId == respPartyId);
        results.setAddressModifiedDate(rs.getString(23));
      }

    } catch (SQLException ex) {
      throw new DAOSysException("SQL Exception thrown\n" + ex.getMessage());
    
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    
    }
    return results;
  }


   private static final String DB_FIND_SIRS_RESP_PARTY_BY_CHILD_ID =
     "SELECT * FROM "
   + "(SELECT * FROM "
   + " (SELECT rp.resp_party_id, rp.resp_area_cd, rp.resp_city_tx, rp.resp_cntry_cd, rp.county_health_juris_id, "
   + "  rp.resp_confirm_dt, rp.resp_first_nm, rp.resp_lang_cd, rp.resp_last_nm, rp.valid_address_fl, rp.stale_addr_fl, rp.resp_middle_nm, "
   + "  rp.resp_notify_fl, rp.resp_phone_tx, rp.resp_phone_ex_tx, rp.valid_phone_fl, rp.resp_state_cd, "
   + "  rp.resp_street_tx, rp.resp_suffix_nm, rp.resp_zip_tx, to_char(NVL(rp.date_modified, rp.date_created), 'yyyymmdd'), "
   + "  rp.record_status_id, to_char(rp.last_addr_date, 'MM/dd/yyyy'), 0 sort_order "
   + "  FROM resp_party_new_view rp "
   + "  WHERE rp.record_status_id = 2 "
   + "  AND rp.child_id = ? "
   + "  UNION "
   + "  SELECT DECODE(rp.record_status_id, 2, rp.resp_party_id, 0), rp.resp_area_cd, rp.resp_city_tx, rp.resp_cntry_cd, rp.county_health_juris_id, "
   + "  rp.resp_confirm_dt, rp.resp_first_nm, rp.resp_lang_cd, rp.resp_last_nm, rp.valid_address_fl, rp.stale_addr_fl, rp.resp_middle_nm, "
   + "  rp.resp_notify_fl, rp.resp_phone_tx, rp.resp_phone_ex_tx, rp.valid_phone_fl, rp.resp_state_cd, "
   + "  rp.resp_street_tx, rp.resp_suffix_nm, rp.resp_zip_tx, to_char(NVL(rp.date_modified, rp.date_created), 'yyyymmdd'), "
   + "  rp.record_status_id, to_char(rp.last_addr_date, 'MM/dd/yyyy'), 99 sort_order "
   + "  FROM resp_party_new_view rp, child c "
   + "  WHERE rp.child_id = c.child_id "
   + "  AND c.resp_party_id = rp.resp_party_id "
   + "  AND rp.record_status_id != 6 "
   + "  AND NVL(c.deleted_fl, 'N') <> 'Y' "
   + "  AND rp.child_id = ? "
   + " ) "
   + " ORDER BY sort_order "
   + ") "
   + "WHERE rownum = 1 ";


  public static RespPartyDetailVO dbFindSirsRespPartyByChildId(long childId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    RespPartyDetailVO results = new RespPartyDetailVO();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt = con.prepareStatement(DB_FIND_SIRS_RESP_PARTY_BY_CHILD_ID);
      logger.info(DB_FIND_SIRS_RESP_PARTY_BY_CHILD_ID);

      stmt.setLong(1, childId);
      stmt.setLong(2, childId);
      logger.info("parms: 1 & 2-" + Long.toString(childId));

      rs = stmt.executeQuery();

      int cnt = 0;
      while(rs.next()) {
        cnt++;
        results.setId(rs.getString(1));
        results.setAreaCodeRaw(rs.getString(2));
        results.setCity(rs.getString(3));
        results.setCountryCode(rs.getString(4));
        results.setCountyHealthJurisId(rs.getString(5));
        results.setConfirmDateRaw(rs.getString(6));
        results.setFirstName(rs.getString(7));
        results.setLanguageCode(rs.getString(8));
        results.setLastName(rs.getString(9));
        results.setMailQualityCode(rs.getString(10));
        results.setStaleAddrFl(rs.getString(11));
        results.setMiddleName(rs.getString(12));
        results.setNotifyCode(rs.getString(13));
        results.setPhoneRaw(rs.getString(14));
        results.setPhoneExtension(rs.getString(15));
        results.setPhoneQualityCode(rs.getString(16));
        results.setStateCode(rs.getString(17));
        results.setAddress(rs.getString(18));
        results.setSuffixName(rs.getString(19));
        results.setZipRaw(rs.getString(20));
        results.setModifiedDateRaw(rs.getString(21));
        results.setRecordStatusId(rs.getString(22));
        results.setAddressModifiedDate(rs.getString(23));
        results.setChildId(childId);
        results.setDefaultFlag(true);
      }

      //if(cnt == 0)  //Bug# 10663
      //throw new DAOSysException("No rows returned.");

    } catch (Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }


  private static final String DB_FLUSH =
    "{call FLUSH.P_FLUSH_CHILD("
  + "p_child_id=>?, p_status_cd=>?, p_status_mg=>?)}";
  
  
  public static void dbFlushChild(long childId)
    throws DAOSysException {

    logger.info(DB_FLUSH);

    int l_status_cd;
      Connection con = null;
      CallableStatement cstmt = null;
    
    try {

      // check input here and throw exceptions if invalid
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();

      cstmt = con.prepareCall(DB_FLUSH);
      
      StringBuffer sb = new StringBuffer();
      sb.append("parms: 1-").append(childId);
      logger.info(sb.toString());
      
       //  register OUT params here
      cstmt.registerOutParameter(2, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(3, java.sql.Types.VARCHAR);
    
      cstmt.setLong(1, childId);
      cstmt.setNull(2, Types.NUMERIC);
      cstmt.setNull(3, Types.VARCHAR);
      
      cstmt.execute();

      l_status_cd = cstmt.getInt(2);
      String l_status_mg = cstmt.getString(3);
      logger.debug("DB returned: status_cd, status_mg: " + l_status_cd + ":" + l_status_mg);
      
    } catch(SQLException ex) {
      logger.error("SQLException thrown. " + ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());
      
    } finally {
       CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
       CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
  }

private static final String DB_GET_HIGH_RISK_RESULTS =
  "SELECT d.disease_id, d.name, d.short_name, d.antigen_id, cd.current_disease_status_id, cd.reported_disease_status_id, "
+ " to_char(cd.date_created, 'YYYYMMDD'), cd.created_by, to_char(cd.date_modified, 'YYYYMMDD'), cd.modified_by "
+ "FROM child_disease cd, disease d "
+ "WHERE cd.disease_id = d.disease_id "
+ "AND cd.child_id = ? ";

public static Collection dbGetHighRiskResults(String childId)
  throws DAOSysException {

  Connection con = null;
  PreparedStatement stmt = null;
  ResultSet rs = null;
  ArrayList results = new ArrayList();

  try {

    con = CoreHelperDAOOracle.getDAOInstance().getConnection();
    stmt = con.prepareStatement(DB_GET_HIGH_RISK_RESULTS);
    logger.info(DB_GET_HIGH_RISK_RESULTS);

    stmt.setString(1, childId);
    logger.info("parms: 1-" + childId);

    rs = stmt.executeQuery();

    while(rs.next()) {
      HighRiskStatusVO hr = new HighRiskStatusVO();
      hr.setChildId(childId);
      hr.setDiseaseId(rs.getString(1));
      hr.setDiseaseName(rs.getString(2));
      hr.setDiseaseShortName(rs.getString(3));
      hr.setAntigenId(rs.getString(4));
      hr.setCurrentStatusId(rs.getString(5));
      hr.setReportedStatusId(rs.getString(6));
      hr.setCreatedDateRaw(rs.getString(7));
      hr.setCreatedBy(rs.getString(8));
      hr.setModifiedDateRaw(rs.getString(9));
      hr.setModifiedBy(rs.getString(10));
      results.add(hr);
    }

  } catch (Exception ex) {
    logger.error(ex.getMessage());
    ex.printStackTrace();
    throw new DAOSysException(ex.getMessage());

  } finally {
    CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
    CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    CoreHelperDAOOracle.getDAOInstance().disConnect(con);
  }

  return results;
}

  private static final String DB_IS_CHILD_CURRENTLY_ACTIVE_ON_SCHOOL_ROSTER =
    "SELECT s.site_id, sr.child_id, ssr.report_period_id "
  + "FROM site s, school_site ss, site_roster sr, school_site_roster ssr "
  + "WHERE s.site_id = ss.site_id "
  + "AND s.site_id = sr.site_id "
  + "AND sr.site_roster_id = ssr.site_roster_id "
  + "AND s.role_id in (0,3) "
  + "AND sr.child_id = ? "
  + "AND ssr.report_period_id IN "
  + "(SELECT min(rp.report_period_id) "
  + " FROM report_period rp, school_site_report_period ssrp, period_status ps, period_type pt "
  + " WHERE rp.report_period_id = ssrp.report_period_id "
  + " AND ssrp.period_status_id = ps.period_status_id "
  + " AND rp.period_type_id = pt.period_type_id "
  + " AND SYSDATE > rp.facility_start_date "
  + " AND pt.name != 'roundup' "
  + " AND ps.name = 'Open' "
  + " AND ssrp.school_site_id = ss.school_site_id)";

  public static boolean dbIsChildCurrentlyActiveOnSchoolRoster(String childId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    boolean results = false;

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt = con.prepareStatement(DB_IS_CHILD_CURRENTLY_ACTIVE_ON_SCHOOL_ROSTER);
      logger.info(DB_IS_CHILD_CURRENTLY_ACTIVE_ON_SCHOOL_ROSTER);

      stmt.setString(1, childId);
      logger.info("parms: 1-" + childId);

      rs = stmt.executeQuery();

      if(rs.next()) results = true;

    } catch (Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }


// jwh - Apparently the jdbc does not recognize Types.BOOLEAN, therefore, 
// this function cannot be called unless the PL/SQL is altered
  private static final String DB_IS_INVALID_ADDRESS =
    "{? = call RESPONSIBLE_PARY_PROCESSING.F_IS_INVALID_ADDRESS("
  + "p_resp_party_id=>?)}";

  public static boolean dbIsInvalidAddress(long respPartyId)
    throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    boolean results = true;

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt = con.prepareCall(DB_IS_INVALID_ADDRESS);
      logger.info(DB_IS_INVALID_ADDRESS);

      // register OUT parameters
      cstmt.registerOutParameter(1, java.sql.Types.BOOLEAN);

      cstmt.setLong(2, respPartyId);
      logger.info("parms: 2-" + respPartyId);

      cstmt.execute();

      results = cstmt.getBoolean(1);

    } catch (Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }

  private static final String DB_PATIENT_ID =
    "SELECT p.patient_id "
  + "FROM patient p, site s "
  + "WHERE p.recsys_id = s.recsys_id "
  + "AND p.child_id = ? "
  + "AND s.site_id = ?";

  public static String dbSelectPatientId(BigDecimal childId, String siteId)
    throws DAOSysException {

    if (childId == null) {
      logger.error("NULL Child Id not allowed.");
      throw new DAOSysException("NULL Child Id not allowed.");
    }
    
    if (siteId == null) {
      logger.error("NULL Site Id not allowed.");
      throw new DAOSysException("NULL Site Id not allowed.");
    }
    
    if (siteId.length() == 0) {
      logger.error("Zero length Site Id not allowed.");
      throw new DAOSysException("Zero length Site Id not allowed.");
    }
    
    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    String result = "";
    
    try {
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();

      stmt = con.prepareStatement(DB_PATIENT_ID);
      stmt.setBigDecimal(1, childId);
      stmt.setString(2, siteId);
      logger.info(DB_PATIENT_ID);
      logger.info("parms: 1-" + childId + " 2-" + siteId) ;

      rs = stmt.executeQuery();
       
      if (rs.next()) {
        result = rs.getString(1);
      }

    } catch (SQLException ex) {
       logger.error(ex.getMessage());
       throw new DAOSysException(ex.getMessage());
    
    } finally {
       CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
       CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
       CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return result;
  }

  private static final String DB_SELECT_EVALUATION_MESSAGE = 
    "SELECT  decode(b.msg_id,501,concat(c.msg_body_tx,to_char(to_date("
  + "DECODE(a.next_shot_dt, NULL, s.ser_rrec_shot_dt),'yyyymmdd'),'mm/dd/yyyy')),c.msg_body_tx) " 
  + "FROM evaluation a, protocol_message b, message_box c, series_eval s "
  + "WHERE a.eval_result_cd = b.eval_result_cd AND a.protocol_id = b.protocol_id "
  + "AND c.msg_id = b.msg_id AND s.child_id = a.child_id "
  + "AND s.series_cd = 20 AND a.protocol_id = 1 "
  + "AND b.protocol_id = 1 AND a.child_id = ?" ;


  public static String dbFindEvaluationMessageByChildId(BigDecimal childId)
    throws DAOSysException {

    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    String results = "";

    try {
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      stmt = con.prepareStatement(DB_SELECT_EVALUATION_MESSAGE);
      logger.info(DB_SELECT_EVALUATION_MESSAGE);
      stmt.setBigDecimal(1, childId);
      rs = stmt.executeQuery();

      if (rs.next()) {
        results = rs.getString(1);
      }

    } catch(Exception ex) {
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());
    
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  } 


  private static final String POPULATE_CHILD =
    "{call POPULATE.P_BEGIN_RETRIEVE("
  + "pi_session_id=>?"
  + ",pi_child_id=>?"
  + ",pi_protocol_id=>?"
  + ",po_status_cd=>?"
  + ",po_status_mg=>?)}";

  public static Status dbPopulateChild(BigDecimal sessionId, long childId, int protocolId)
    throws DAOSysException {
  
    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();
    
    try {

      // check input here and throw exceptions if invalid
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt = con.prepareCall(POPULATE_CHILD);
      logger.info(POPULATE_CHILD);
      
      //  register OUT params here
      cstmt.registerOutParameter(4, java.sql.Types.BIGINT);
      cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
      
      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, childId);
      cstmt.setInt(3, protocolId);
      
      StringBuffer sb = new StringBuffer();
      sb.append("parms:");
      sb.append(" 1-").append(sessionId);
      sb.append(" 2-").append(Long.toString(childId));
      sb.append(" 3-").append(Integer.toString(protocolId));
      logger.info(sb.toString());
      
      cstmt.execute();

      long l_status_cd = cstmt.getLong(4);         // the db puts the child_id here, if successful
      String l_status_mg = cstmt.getString(5);
      results.setDbMessage(l_status_mg);
      results.setConverted(true);
      logger.info("cd,mg: " + l_status_cd + ":" + l_status_mg);

      if (l_status_cd < 1) {
        results.setValid(false);
        results.setCode(l_status_cd);
        sb = new StringBuffer();
        sb.append(ErrorConstants.CHILD_POPULATE).append(Math.abs(l_status_cd));
        results.setMessage(sb.toString());
        logger.error(results);
      }
      
    } catch(Exception ex) {
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }

  private static final String POST_ASSESS_CHILD =
    "{call ASSESSMENT.P_POST_ASSESSMENT("
  + "pi_session_id=>?"
  + ",pi_child_id=>?"
  + ",pi_protocol_id=>?"
  + ",pi_batch_fl=>?"
  + ",pi_force_fl=>'Y'"
  + ",pi_days_prior=>?"
  + ",po_status_cd=>?"
  + ",po_status_mg=>?)}";

  public static Status dbPostAssessChild(BigDecimal sessionId, long childId, Integer protocolId, int daysPrior, boolean isBatch)
    throws DAOSysException {
  
    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();
    
    try {

      // check input here and throw exceptions if invalid
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt = con.prepareCall(POST_ASSESS_CHILD);
      logger.info(POST_ASSESS_CHILD);
      
      //  register OUT params here
      cstmt.registerOutParameter(6, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(7, java.sql.Types.VARCHAR);
      
      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, childId);
      if (protocolId==null)
          cstmt.setNull(3, Types.NUMERIC);
      else
          cstmt.setInt(3, protocolId);
      cstmt.setString(4, isBatch ? "Y" : "N");
      cstmt.setInt(5, daysPrior);
      
      StringBuffer sb = new StringBuffer();
      sb.append("parms:");
      sb.append(" 1-").append(sessionId);
      sb.append(" 2-").append(Long.toString(childId));
      sb.append(" 3-").append(protocolId);
      sb.append(" 4-").append(isBatch ? "Y" : "N");
      sb.append(" 5-").append(Integer.toString(daysPrior));
      logger.info(sb.toString());
      
      cstmt.execute();

      int l_status_cd = cstmt.getInt(6);         // the db puts the child_id here, if successful
      String l_status_mg = cstmt.getString(7);
      results.setDbMessage(l_status_mg);
      results.setConverted(true);
      logger.info("cd,mg: " + l_status_cd + ":" + l_status_mg);

      if (l_status_cd < 1) {
        results.setValid(false);
        results.setCode(l_status_cd);
        sb = new StringBuffer();
        sb.append(ErrorConstants.CHILD_POPULATE).append(Math.abs(l_status_cd));
        results.setMessage(sb.toString());
        logger.error(results);
      }
      
    } catch(Exception ex) {
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }

  private static final String P_POST_CHILD =
    "{call CHILD_INFORMATION_PROCESSING.P_POST_CHILD ( " 
  + "pi_session_id=>?, pi_child_id=>?, pi_last_nm=>?, pi_first_nm=>?, pi_middle_nm=>?, pi_suffix_nm=>?, "
  + "pi_alt_last_nm=>?, pi_alt_first_nm=>?, pi_moms_maiden_nm=>?, "
  + "pi_dob=>?, pi_sex_cd=>?, pi_plurality_cd=>?, pi_birth_order=>?, "
  + "pi_state_cd=>?, pi_county_health_juris_id=>?, "
  + "pi_birth_hosp_nm=>?, pi_birth_state_cd=>?, pi_birth_cnty_cd=>?, "
  + "pi_patient_id=>?, pi_wic_id=>?, pi_birth_certificate_fl=>?, "
  + "pi_grade_id=>?, pi_last_physical_date=>?, pi_vision_fl=>?, "
  + "pi_active_fl=>?, pi_round_up_fl=>?, pi_report_period_id=>?, "
  + "pi_medicaid_id=>?, pi_uic_id=>?, " /* pi_health_plan_nm=>? */
  + "pi_mi_prov_fl=>?, pi_death_fl=>?, "
  + "pi_migrant_fl=>?, pi_alt_name_fl=>?, pi_optout_fl=>?, "
  + "pi_moge_fl=>?, pi_provider_moge_status_id=>?, pi_region_moge_status_id=>?, "
  + "pi_hearing_fl=>?, pi_mvha_eligible_fl=>?, "
  + "pi_resp_party_id=>?, pi_resp_street_tx=>?, pi_resp_city_tx=>?, pi_resp_state_cd=>?, pi_resp_zip_tx=>?, "
  + "pi_resp_cntry_cd=>?, pi_resp_area_cd=>?, pi_resp_phone_tx=>?, pi_resp_phone_ex_tx=>?, pi_resp_lang_cd=>?, "
  + "pi_child_disease_str=>?, pi_bmi_opt_out_fl=>?, "
  + "po_status_cd=>?, po_status_mg=>?)}";

  private static final String P_POST_ADD_CHILD =
    "{CALL CHILD_INFORMATION_PROCESSING.P_POST_ADD_CHILD ( " 
  + "pi_session_id=>?, pi_last_nm=>?, pi_first_nm=>?, pi_middle_nm=>?, pi_suffix_nm=>?, "
  + "pi_alt_last_nm=>?, pi_alt_first_nm=>?, pi_moms_maiden_nm=>?, "
  + "pi_patient_id=>?, pi_medicaid_id=>?, pi_wic_id=>?, "
  + "pi_dob=>?, pi_sex_cd=>?, pi_plurality_cd=>?, pi_birth_order=>?, "
  + "pi_state_cd=>?, pi_county_health_juris_id=>?, "
  + "pi_birth_hosp_nm=>?, pi_birth_state_cd=>?, pi_birth_cnty_cd=>?, "
  + "pi_resp_last_nm=>?, pi_resp_first_nm=>?, pi_resp_middle_nm=>?, pi_resp_suffix_nm=>?, "
  + "pi_resp_street_tx=>?, pi_resp_city_tx=>?, pi_resp_state_cd=>?, pi_resp_cntry_cd=>?, "
  + "pi_resp_zip_tx=>?, pi_resp_area_cd=>?, pi_resp_phone_tx=>?, pi_resp_phone_ex_tx=>?, "
  + "pi_resp_notify_cd=>?, pi_resp_lang_cd=>?, "
  + "po_child_id=>?, po_resp_party_id=>?, po_status_cd=>?, po_status_mg=>?)}";


  public static Status dbPostChild(BigDecimal sessionId, ChildDetailExtendedVO child, RespPartyDetailVO respParty, SiteChildVO siteChild, String highRiskDelimitedList)
    throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();
    
    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      // check input here and throw exceptions if invalid
      if (child != null && child.getId().equals("0")) {
        // add child
        cstmt = con.prepareCall(P_POST_ADD_CHILD);
        logger.info(P_POST_ADD_CHILD);

        // Register OUT parameters
        cstmt.registerOutParameter(35, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(36, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(37, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(38, java.sql.Types.VARCHAR);

        cstmt.setBigDecimal(1, sessionId);
        cstmt.setString(2, child.getLastName());
        cstmt.setString(3, child.getFirstName());
        cstmt.setString(4, child.getMiddleName());
        cstmt.setString(5, child.getSuffixName());
        cstmt.setString(6, child.getAliasLastName());
        cstmt.setString(7, child.getAliasFirstName());
        cstmt.setString(8, child.getMothersMaidenName());
        cstmt.setString(9, siteChild.getPatientId());
        if(child.getMedicaidId().length() > 0) {
          cstmt.setLong(10, Long.parseLong(child.getMedicaidId()));
        } else {
          cstmt.setNull(10, Types.NUMERIC);
        }
        cstmt.setString(11, child.getWicId());
        cstmt.setString(12, child.getBirthDateRaw());
        cstmt.setString(13, child.getGender());
        cstmt.setString(14, child.getBirthQuantity());
        if(child.getBirthOrder().length() > 0) {
          cstmt.setInt(15, Integer.parseInt(child.getBirthOrder()));
        } else {
          cstmt.setNull(15, Types.NUMERIC);
        }
        cstmt.setString(16, respParty.getStateCode());
        // This check is required for adding out of state children
        if(respParty.getCountyHealthJurisId().length() > 0) {
          cstmt.setInt(17, Integer.parseInt(respParty.getCountyHealthJurisId()));
        } else {
          cstmt.setNull(17, Types.NUMERIC);
        }
        cstmt.setString(18, child.getFacilityName());
        cstmt.setString(19, child.getFacilityStateCode());
        cstmt.setString(20, child.getFacilityCountyCode());
        cstmt.setString(21, respParty.getLastName());
        cstmt.setString(22, respParty.getFirstName());
        cstmt.setString(23, respParty.getMiddleName());
        cstmt.setString(24, respParty.getSuffixName());
        cstmt.setString(25, respParty.getAddress());
        cstmt.setString(26, respParty.getCity());
        cstmt.setString(27, respParty.getStateCode());
        cstmt.setString(28, respParty.getCountryCode());
        cstmt.setString(29, respParty.getZipRaw());
        cstmt.setString(30, respParty.getAreaCodeRaw());
        cstmt.setString(31, respParty.getPhoneRaw());
        cstmt.setString(32, respParty.getPhoneExtension());
        cstmt.setString(33, respParty.getNotifyCode());
        cstmt.setString(34, respParty.getLanguageCode());
        cstmt.setNull(35, Types.NUMERIC);
        cstmt.setNull(36, Types.NUMERIC);
        cstmt.setNull(37, Types.NUMERIC);
        cstmt.setNull(38, Types.VARCHAR);

        StringBuffer sb = new StringBuffer();
        sb.append("parms:");
        sb.append(" 1-").append(sessionId);
        sb.append(" 2-").append(child.getLastName());
        sb.append(" 3-").append(child.getFirstName());
        sb.append(" 4-").append(child.getMiddleName());
        sb.append(" 5-").append(child.getSuffixName());
        sb.append(" 6-").append(child.getAliasLastName());
        sb.append(" 7-").append(child.getAliasFirstName());
        sb.append(" 8-").append(child.getMothersMaidenName());
        sb.append(" 9-").append(siteChild.getPatientId());
        sb.append(" 10-").append(child.getMedicaidId());
        sb.append(" 11-").append(child.getWicId());
        sb.append(" 12-").append(child.getBirthDateRaw());
        sb.append(" 13-").append(child.getGender());
        sb.append(" 14-").append(child.getBirthQuantity());
        sb.append(" 15-").append(child.getBirthOrder());
        sb.append(" 16-").append(respParty.getStateCode());
        sb.append(" 17-").append(respParty.getCountyHealthJurisId());
        sb.append(" 18-").append(child.getFacilityName());
        sb.append(" 19-").append(child.getFacilityStateCode());
        sb.append(" 20-").append(child.getFacilityCountyCode());
        sb.append(" 21-").append(respParty.getLastName());
        sb.append(" 22-").append(respParty.getFirstName());
        sb.append(" 23-").append(respParty.getMiddleName());
        sb.append(" 24-").append(respParty.getSuffixName());
        sb.append(" 25-").append(respParty.getAddress());
        sb.append(" 26-").append(respParty.getCity());
        sb.append(" 27-").append(respParty.getStateCode());
        sb.append(" 28-").append(respParty.getCountryCode());
        sb.append(" 29-").append(respParty.getZipRaw());
        sb.append(" 30-").append(respParty.getAreaCodeRaw());
        sb.append(" 31-").append(respParty.getPhoneRaw());
        sb.append(" 32-").append(respParty.getPhoneExtension());
        sb.append(" 33-").append(respParty.getNotifyCode());
        sb.append(" 34-").append(respParty.getLanguageCode());
        logger.info(sb.toString());

        cstmt.execute();
        
        // get results from proc  
        long childId = cstmt.getLong(35);
        int status_cd = cstmt.getInt(37);
        String status_mg = cstmt.getString(38);

        results.setMessage(status_mg);
        if (status_cd > 0) {
          results.setObject(Long.toString(childId));

        } else {
          results.setCode(status_cd);
          results.setValid(false);
        }
        
        if (logger.isInfoEnabled()) {
          logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
        }

      } else {
        // update child

        cstmt = con.prepareCall(P_POST_CHILD);
        logger.info(P_POST_CHILD);

        // Register OUT parameters
        cstmt.registerOutParameter(52, java.sql.Types.NUMERIC);
        cstmt.registerOutParameter(53, java.sql.Types.VARCHAR);

        cstmt.setBigDecimal(1, sessionId);
        cstmt.setLong(2, Long.parseLong(child.getId()));
        cstmt.setString(3, child.getLastName());
        cstmt.setString(4, child.getFirstName());
        cstmt.setString(5, child.getMiddleName());
        cstmt.setString(6, child.getSuffixName());
        cstmt.setString(7, child.getAliasLastName());
        cstmt.setString(8, child.getAliasFirstName());
        cstmt.setString(9, child.getMothersMaidenName());
        cstmt.setString(10, child.getBirthDateRaw());
        cstmt.setString(11, child.getGender());
        cstmt.setString(12, child.getBirthQuantity());
        if(child.getBirthOrder().length() > 0) {
          cstmt.setInt(13, Integer.parseInt(child.getBirthOrder()));
        } else {
          cstmt.setNull(13, Types.NUMERIC);
        }
        cstmt.setString(14, child.getStateCode());
        if(child.getCountyHealthJurisdictionId().length() > 0) {
          cstmt.setInt(15, Integer.parseInt(child.getCountyHealthJurisdictionId()));
        } else {
          cstmt.setNull(15, Types.NUMERIC);
        }
        cstmt.setString(16, child.getFacilityName());
        cstmt.setString(17, child.getFacilityStateCode());
        cstmt.setString(18, child.getFacilityCountyCode());
        cstmt.setString(19, siteChild.getPatientId());
        cstmt.setString(20, child.getWicId());
        cstmt.setString(21, siteChild.hasBirthCertificate() ? "Y" : "N" );
        cstmt.setString(22, child.getGradeId());
        cstmt.setString(23, child.getLastPhysicalDateRaw());
        cstmt.setString(24, child.isVisionScreened() ? "Y" : "N");
        cstmt.setString(25, siteChild.isActive() ? "Y" : "N");
        cstmt.setString(26, siteChild.isRoundup() ? "Y" : "N");
        cstmt.setString(27, siteChild.getReportPeriodId());
        if(child.getMedicaidId().length() > 0) {
          cstmt.setLong(28, Long.parseLong(child.getMedicaidId()));
        } else {
          cstmt.setNull(28, Types.NUMERIC);
        }
        cstmt.setNull(29, Types.VARCHAR); // uic
//        cstmt.setString(12, child.getHealthPlanName());
        cstmt.setString(30, child.getProviderFlag());
        cstmt.setString(31, child.getDeathFlag());
        cstmt.setString(32, child.getMigrantFlag());
        cstmt.setString(33, child.getAliasFlag());
        cstmt.setString(34, child.getOptOutFlag());
        cstmt.setString(35, child.isMovedOrGoneElsewhere() ? "Y" : "N");
        cstmt.setString(36, child.getProviderMogeStatusId());
        cstmt.setString(37, child.getRegionMogeStatusId());
        cstmt.setString(38, child.isHearingScreened() ? "Y" : "N");
        cstmt.setString(39, child.isMvhaEligible() ? "Y" : "N");

        // Resp Party Info
        if(respParty != null) {
          cstmt.setLong(40, Long.parseLong(respParty.getId()));
          cstmt.setString(41, respParty.getAddress());
          cstmt.setString(42, respParty.getCity());
          cstmt.setString(43, respParty.getStateCode());
          cstmt.setString(44, respParty.getZipRaw());
          cstmt.setString(45, respParty.getCountryCode());
          cstmt.setString(46, respParty.getAreaCodeRaw());
          cstmt.setString(47, respParty.getPhoneRaw());
          cstmt.setString(48, respParty.getPhoneExtension());
          cstmt.setString(49, respParty.getLanguageCode());
        } else {
          cstmt.setNull(40, Types.NUMERIC);
          cstmt.setNull(41, Types.VARCHAR);
          cstmt.setNull(42, Types.VARCHAR);
          cstmt.setNull(43, Types.VARCHAR);
          cstmt.setNull(44, Types.VARCHAR);
          cstmt.setNull(45, Types.VARCHAR);
          cstmt.setNull(46, Types.VARCHAR);
          cstmt.setNull(47, Types.VARCHAR);
          cstmt.setNull(48, Types.VARCHAR);
          cstmt.setNull(49, Types.VARCHAR);
        }

        cstmt.setString(50, highRiskDelimitedList);
        cstmt.setString(51, child.getBmiOptOutFlag());

        StringBuffer sb = new StringBuffer();
        sb.append("parms:");
        sb.append(" 1-").append(sessionId);
        sb.append(" 2-").append(child.getId());
        sb.append(" 3-").append(child.getLastName());
        sb.append(" 4-").append(child.getFirstName());
        sb.append(" 5-").append(child.getMiddleName());
        sb.append(" 6-").append(child.getSuffixName());
        sb.append(" 7-").append(child.getAliasLastName());
        sb.append(" 8-").append(child.getAliasFirstName());
        sb.append(" 9-").append(child.getMothersMaidenName());
        sb.append(" 10-").append(child.getBirthDateRaw());
        sb.append(" 11-").append(child.getGender());
        sb.append(" 12-").append(child.getBirthQuantity());
        sb.append(" 13-").append(child.getBirthOrder());
        sb.append(" 14-").append(child.getStateCode());
        sb.append(" 15-").append(child.getCountyHealthJurisdictionId());
        sb.append(" 16-").append(child.getFacilityName());
        sb.append(" 17-").append(child.getFacilityStateCode());
        sb.append(" 18-").append(child.getFacilityCountyCode());
        sb.append(" 19-").append(siteChild.getPatientId());
        sb.append(" 20-").append(child.getWicId());
        sb.append(" 21-").append(siteChild.hasBirthCertificate() ? "Y" : "N");
        sb.append(" 22-").append(child.getGradeId());
        sb.append(" 23-").append(child.getLastPhysicalDateRaw());
        sb.append(" 24-").append(child.isVisionScreened() ? "Y" : "N");
        sb.append(" 25-").append(siteChild.isActive() ? "Y" : "N");
        sb.append(" 26-").append(siteChild.isRoundup() ? "Y" : "N");
        sb.append(" 27-").append(siteChild.getReportingYearId());
        sb.append(" 28-").append(child.getMedicaidId());
//        sb.append(" 10-").append(child.getHealthPlanName());
        sb.append(" 30-").append(child.getProviderFlag());
        sb.append(" 31-").append(child.getDeathFlag());
        sb.append(" 32-").append(child.getMigrantFlag());
        sb.append(" 33-").append(child.getAliasFlag());
        sb.append(" 34-").append(child.getOptOutFlag());
        sb.append(" 35-").append(child.isMovedOrGoneElsewhere() ? "Y" : "N");
        sb.append(" 36-").append(child.getProviderMogeStatusId());
        sb.append(" 37-").append(child.getRegionMogeStatusId());
        sb.append(" 38-").append(child.isHearingScreened() ? "Y" : "N");
        sb.append(" 39-").append(child.isMvhaEligible() ? "Y" : "N");
        if(respParty != null) {
          sb.append(" 40-").append(respParty.getId());
          sb.append(" 41-").append(respParty.getAddress());
          sb.append(" 42-").append(respParty.getCity());
          sb.append(" 43-").append(respParty.getStateCode());
          sb.append(" 44-").append(respParty.getZipRaw());
          sb.append(" 45-").append(respParty.getCountryCode());
          sb.append(" 46-").append(respParty.getAreaCodeRaw());
          sb.append(" 47-").append(respParty.getPhoneRaw());
          sb.append(" 48-").append(respParty.getPhoneExtension());
          sb.append(" 49-").append(respParty.getLanguageCode());
        }
        sb.append(" 50-").append(highRiskDelimitedList);
        sb.append(" 51-").append(child.getBmiOptOutFlag());
        logger.info(sb.toString());

        cstmt.execute();
        
        // get results from proc  
        int status_cd = cstmt.getInt(52);
        String status_mg = cstmt.getString(53);
        
        results.setMessage(status_mg);
        if (status_cd < 1) {

          results.setCode(status_cd);
          results.setValid(false);
        }
        
        if (logger.isInfoEnabled()) {
          logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
        }

      }

    } catch(Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());
      
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;

  }

private static final String POST_HIGH_RISK_RESULTS =
  "{call CHILD_DISEASE_PKG.P_POST_CHILD_DISEASE("
+ "pi_session_id=>?,"
+ "pi_record_str=>?,"
+ "pi_remove_fl=>?,"
+ "po_result_str=>?,"
+ "po_status_cd=>?,"
+ "po_status_mg=>?)}";

public static Status dbPostHighRiskResults(BigDecimal sessionId, String highRiskDelimitedList)
  throws DAOSysException {

  Connection con = null;
  CallableStatement cstmt = null;
  Status results = new Status();

  try {

    con = CoreHelperDAOOracle.getDAOInstance().getConnection();
    cstmt = con.prepareCall(POST_HIGH_RISK_RESULTS);
    logger.info(POST_HIGH_RISK_RESULTS);

    cstmt.registerOutParameter(4, Types.VARCHAR);
    cstmt.registerOutParameter(5, Types.NUMERIC);
    cstmt.registerOutParameter(6, Types.VARCHAR);

    cstmt.setBigDecimal(1, sessionId);
    cstmt.setString(2, highRiskDelimitedList);
    cstmt.setString(3, "N");  // pi_remove_fl

    StringBuffer sb = new StringBuffer();
    sb.append("parms: 1-").append(sessionId);
    sb.append(" 2-").append(highRiskDelimitedList);
    sb.append(" 3-").append("N");
    logger.info(sb.toString());

    cstmt.execute();

    String resultString = cstmt.getString(4);
    int status_cd = cstmt.getInt(5);
    String status_mg = cstmt.getString(6);

    logger.info("Status Code: " + status_cd + ", Status Msg: " + status_mg);

    results.setMessage(status_mg);
    results.setObject(resultString);
    if(status_cd < 1) {
      results.setValid(false);
      results.setCode(status_cd);
      logger.error(results);
      logger.error(resultString);

      // Add code here to parse the resultString if line-level error results are required
    }

  } catch (Exception ex) {
    logger.error(ex.getMessage());
    ex.printStackTrace();
    throw new DAOSysException(ex.getMessage());

  } finally {
    CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
    CoreHelperDAOOracle.getDAOInstance().disConnect(con);
  }

  return results;
}

  private static final String POST_PIT_ASSESS_CHILD =
    "{call ASSESSMENT.P_POST_ASSESSMENT("
  + "pi_session_id=>?,"
  + "pi_child_id=>?,"
  + "pi_protocol_id=>?,"
  + "pi_batch_fl=>?,"
  + "pi_days_prior=>?,"
  + "pi_as_of_dt=>?,"
  + "pi_request_id=>?,"
  + "po_status_cd=>?,"
  + "po_status_mg=>?)}";

  public static Status dbPostPitAssessChild(BigDecimal sessionId, long requestStatusId, long childId, int protocolId, int daysPrior, String asOfDate)
    throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt = con.prepareCall(POST_PIT_ASSESS_CHILD);
      logger.info(POST_PIT_ASSESS_CHILD);

      cstmt.registerOutParameter(8, Types.NUMERIC);
      cstmt.registerOutParameter(9, Types.VARCHAR);

      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, childId);
      cstmt.setInt(3, protocolId);
      cstmt.setString(4, "Y");
      cstmt.setInt(5, daysPrior);
      cstmt.setString(6, asOfDate);
      cstmt.setLong(7, requestStatusId);

      StringBuffer sb = new StringBuffer();
      sb.append("parms: 1-").append(sessionId);
      sb.append(" 2-").append(childId);
      sb.append(" 3-").append(protocolId);
      sb.append(" 4-").append("Y");
      sb.append(" 5-").append(daysPrior);
      sb.append(" 6-").append(asOfDate);
      sb.append(" 7-").append(requestStatusId);
      logger.info(sb.toString());

      cstmt.execute();

      int l_status_cd = cstmt.getInt(8);
      String l_status_mg = cstmt.getString(9);
      results.setDbMessage(l_status_mg);
      results.setConverted(true);
      logger.debug("Status Cd: " + l_status_cd + ", Status Msg: " + l_status_mg);
      if(l_status_cd < 1) {
        results.setValid(false);
        results.setCode(l_status_cd);
        sb = new StringBuffer();
        sb.append(ErrorConstants.CHILD_POPULATE).append(Math.abs(l_status_cd));
        results.setMessage(sb.toString());
        logger.error(results);
      }

    } catch (Exception ex) {
      logger.error(ex);
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }


  private static final String P_POST_RESP_PARTY =
    "{call RESPONSIBLE_PARTY_PROCESSING.P_POST_RESP_PARTY("
  + "pi_session_id=>?, "
  + "pio_resp_party_id=>?, "
  + "pi_child_id=>?, "
  + "pi_resp_first_nm=>?, "
  + "pi_resp_last_nm=>?, "
  + "pi_resp_middle_nm=>?, "
  + "pi_resp_suffix_nm=>?, "
  + "pi_resp_street_tx=>?, "
  + "pi_resp_city_tx=>?, "
  + "pi_county_health_juris_id=>?, "
  + "pi_resp_state_cd=>?, "
  + "pi_resp_zip_tx=>?, "
  + "pi_resp_cntry_cd=>?, "
//  + "pi_resp_email_tx=>?, "
  + "pi_resp_area_cd=>?, "
  + "pi_resp_phone_tx=>?, "
  + "pi_resp_lang_cd=>?, "
  + "pi_resp_notify_cd=>?, "
  + "pi_remove_fl=>?, "
  + "pi_current_resp_party_fl=>?, "
  + "po_status_cd=>?, "
  + "po_status_mg=>?)}";

  public static Status dbPostRespParty(BigDecimal sessionId, RespPartyDetailVO respParty, boolean removeFlag, boolean currentFlag)
    throws DAOSysException {

    Connection con = null;
    CallableStatement cstmt = null;
    Status results = new Status();

    try {

      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
      cstmt = con.prepareCall(P_POST_RESP_PARTY);
      logger.info(P_POST_RESP_PARTY);

      // Register OUT parameters
      cstmt.registerOutParameter(2, Types.NUMERIC);
      cstmt.registerOutParameter(20, Types.NUMERIC);
      cstmt.registerOutParameter(21, Types.VARCHAR);

      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, Long.parseLong(respParty.getId()));
      cstmt.setLong(3, Long.parseLong(respParty.getChildId()));
      cstmt.setString(4, respParty.getFirstName());
      cstmt.setString(5, respParty.getLastName());
      cstmt.setString(6, respParty.getMiddleName());
      cstmt.setString(7, respParty.getSuffixName());
      cstmt.setString(8, respParty.getAddress());
      cstmt.setString(9, respParty.getCity());
      if(respParty.getCountyHealthJurisId().length() > 0)
        cstmt.setInt(10, Integer.parseInt(respParty.getCountyHealthJurisId()));
      else
        cstmt.setNull(10, Types.NUMERIC);
      cstmt.setString(11, respParty.getStateCode());
      cstmt.setString(12, respParty.getZipRaw());
      cstmt.setString(13, respParty.getCountryCode());
      cstmt.setString(14, respParty.getAreaCodeRaw());
      cstmt.setString(15, respParty.getPhoneRaw());
      cstmt.setString(16, respParty.getLanguageCode());
      cstmt.setString(17, respParty.getNotifyCode());
      cstmt.setString(18, removeFlag ? "Y" : "N");
      cstmt.setString(19, currentFlag ? "Y" : "N");

      StringBuffer sb = new StringBuffer();
      sb.append("parms:");
      sb.append(" 1-").append(sessionId);
      sb.append(" 2-").append(respParty.getId());
      sb.append(" 3-").append(respParty.getChildId());
      sb.append(" 4-").append(respParty.getFirstName());
      sb.append(" 5-").append(respParty.getLastName());
      sb.append(" 6-").append(respParty.getMiddleName());
      sb.append(" 7-").append(respParty.getSuffixName());
      sb.append(" 8-").append(respParty.getAddress());
      sb.append(" 9-").append(respParty.getCity());
      sb.append(" 10-").append(respParty.getCountyHealthJurisId());
      sb.append(" 11-").append(respParty.getStateCode());
      sb.append(" 12-").append(respParty.getZipRaw());
      sb.append(" 13-").append(respParty.getCountryCode());
      sb.append(" 14-").append(respParty.getAreaCodeRaw());
      sb.append(" 15-").append(respParty.getPhoneRaw());
      sb.append(" 16-").append(respParty.getLanguageCode());
      sb.append(" 17-").append(respParty.getNotifyCode());
      sb.append(" 18-").append(removeFlag ? "Y" : "N");
      sb.append(" 19-").append(currentFlag ? "Y" : "N");
      logger.info(sb.toString());

      cstmt.execute();

      // get results from proc
      long respPartyId = cstmt.getLong(2);
      int status_cd = cstmt.getInt(20);
      String status_mg = cstmt.getString(21);

      results.setMessage(status_mg);
      if(status_cd > 0) {
        results.setObject(Long.toString(respPartyId));

      } else {
        results.setCode(status_cd);
        results.setValid(false);
      }

      if(logger.isInfoEnabled()) {
        logger.info("Status Code: " + status_cd + ", Status Msg: " + status_mg);
      }

    } catch(Exception ex) {
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }

    return results;
  }


  private static final String UPDATE_LEAD_STATUS =
    "{call CHILD_INFORMATION_PROCESSING.P_UPDATE_LEAD_STATUS("
  + "pi_session_id=>?"
  + ",pi_child_id=>?"
  + ",pi_lead_status_fl=>?"
  + ",pio_status_cd=>?"
  + ",pio_status_mg=>?)}";

  public static Status dbUpdateLeadStatus(BigDecimal sessionId, long childId, String leadFlag) {

    Connection con = null;  
    CallableStatement cstmt = null;
    Status results = new Status();

    try {
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
    
      //takes 5 arguments
      cstmt = con.prepareCall(UPDATE_LEAD_STATUS);
      if (logger.isDebugEnabled()) {
        logger.debug("{call UPDATE_LEAD_STATUS(w/ 3-IN 2-OUT arguments)} ");
      }
      cstmt.registerOutParameter(4, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
    
      cstmt.setBigDecimal(1, sessionId);
      cstmt.setLong(2, childId);
      cstmt.setString(3, leadFlag);
      cstmt.setNull(4, Types.NUMERIC);  //status_cd
      cstmt.setNull(5, Types.VARCHAR);  //status_mg
      

      StringBuffer sb = new StringBuffer();
      sb.append("parms 1-").append(sessionId);
      sb.append(" 2-").append(childId);
      sb.append(" 3-").append(leadFlag);
      logger.debug(sb.toString());
      
      cstmt.execute();
      
      // get results from proc  
      int status_cd = cstmt.getInt(4);
      String status_mg = cstmt.getString(5);
      
      results.setMessage(status_mg);
      if (status_cd < 1) {
        results.setCode(status_cd);
        results.setValid(false);
      }
      
      if (logger.isInfoEnabled()) {
        logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
      }

    } catch(Exception ex) {
      throw new DAOSysException(ex.getMessage());
    
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }
  
  private static final String UPDATE_ROSTER =
    "{call CHILD_ROSTER.P_POST_SITE_ROSTER("
  + " pi_session_id=>?"
  + ",pi_site_id=>?"
  + ",pi_child_id=>?"
  + ",pi_report_period_id=>?"
  + ",pi_remove_fl=>?"
  + ",po_status_cd=>?"
  + ",po_status_mg=>?)}";

  public static Status dbUpdateRoster(BigDecimal sessionId, String siteId, long childId, String reportPeriodId, boolean removeFlag)
    throws DAOSysException {

    Connection con = null;  
    CallableStatement cstmt = null;
    Status results = new Status();

    try {
      con = CoreHelperDAOOracle.getDAOInstance().getConnection();
    
      cstmt = con.prepareCall(UPDATE_ROSTER);
      logger.info(UPDATE_ROSTER);

      cstmt.registerOutParameter(6, java.sql.Types.NUMERIC);
      cstmt.registerOutParameter(7, java.sql.Types.VARCHAR);
    
      cstmt.setBigDecimal(1, sessionId);
      cstmt.setString(2, siteId);
      cstmt.setLong(3, childId);
      if(reportPeriodId.length() > 0)
        cstmt.setLong(4, Long.parseLong(reportPeriodId));  // Only used for schools/childcares when doing a find child
      else
        cstmt.setNull(4, Types.NUMERIC);
      cstmt.setString(5, removeFlag ? "Y" : "N");
      cstmt.setNull(6, Types.NUMERIC);
      cstmt.setNull(7, Types.VARCHAR);
      
      StringBuffer sb = new StringBuffer();
      sb.append("parms 1-").append(sessionId);
      sb.append(" 2-").append(siteId);
      sb.append(" 3-").append(childId);
      sb.append(" 4-").append(reportPeriodId);
      sb.append(" 5-").append(removeFlag ? "Y" : "N");
      logger.info(sb.toString());
      
      cstmt.execute();
      
      int status_cd = cstmt.getInt(6);
      String status_mg = cstmt.getString(7);
      results.setConverted(true);
      results.setDbMessage(status_mg);
      if (logger.isInfoEnabled()) {
        logger.info("Status Code: " + status_cd + ", Status Msg:" + status_mg);
      }

      if (status_cd < 1) {
        results.setValid(false);
        results.setCode(status_cd);
        sb = new StringBuffer();
        sb.append(ErrorConstants.CHILD_ROSTER).append(Math.abs(status_cd));
        results.setMessage(sb.toString());
        logger.error(results);
      
      } else {
        results.setMessage(ErrorConstants.UPDATE_SUCCESSFUL);
      }
      
    } catch(Exception ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      throw new DAOSysException(ex.getMessage());
    
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeStatement(cstmt);
      CoreHelperDAOOracle.getDAOInstance().disConnect(con);
    }
    return results;
  }
 
  private final static String DB_GET_DEDUPLICATION_PERSONS =
	        "SELECT DISTINCT BASE_CHILD_ID FROM DUP_MATCH_RESULT WHERE MATCH_STATUS = 1 "
	      + " UNION "
	      + "SELECT DISTINCT MATCH_CHILD_ID FROM DUP_MATCH_RESULT WHERE MATCH_STATUS = 1 ";
  
  public static List<String> getDeduplicationPersons() throws DAOSysException {

  Connection con = null;
  PreparedStatement stmt = null;
  ResultSet rs = null;
  ArrayList<String> results = new ArrayList<String>();

  try {

    con = CoreHelperDAOOracle.getDAOInstance().getConnection();
    stmt = con.prepareStatement(DB_GET_DEDUPLICATION_PERSONS);
    logger.info(DB_GET_DEDUPLICATION_PERSONS);

    rs = stmt.executeQuery();

    while(rs.next()) {
      results.add(rs.getLong(1)+"");
     }

  } catch (Exception ex) {
    logger.error(ex.getMessage());
    ex.printStackTrace();
    throw new DAOSysException(ex.getMessage());

  } finally {
    CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
    CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    CoreHelperDAOOracle.getDAOInstance().disConnect(con);
  }

  return results;
}
  
  //#########################################
  //
  //  DupMatchResult
  //
  //#########################################

  public static DupMatchResult dbFindDupMatchResultByPrimaryKey(long id, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    DupMatchResult results = null;

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(DupMatchResult.SQL_FIND_DUP_MATCH_RESULT).append(DupMatchResult.SQL_FILTER_DUP_MATCH_RESULT_ID);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, id);

      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(id);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        results = PatientHelperDAOOracle.getDupMatchResultResults(rs);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException", e);

    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }

  public static List<DupMatchResult> dbFindDupMatchResultListByRegionIdCountyHealthJurisId(Long regionId, Long countyHealthJurisId, Connection con)
    throws DAOSysException {

      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<DupMatchResult> results = new ArrayList<DupMatchResult>();

      try {
        if (con==null)
          CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
        logger.debug("using con: " + con);
        logger.debug("Autocommit state: " + con.getAutoCommit());

        StringBuffer sb = new StringBuffer();
        // TODO
        sb.append(DupMatchResult.SQL_FIND_DUP_MATCH_RESULT).append(DupMatchResult.SQL_FILTER_DEDUP_ROSTER);
        stmt = con.prepareStatement(sb.toString());
        stmt.setLong(1, regionId);
        stmt.setLong(2, regionId);
        if(countyHealthJurisId == null) {
          stmt.setNull(3, Types.NUMERIC);
          stmt.setNull(4, Types.NUMERIC);
        } else { 
          stmt.setLong(3, countyHealthJurisId);
          stmt.setLong(4, countyHealthJurisId);
        }

        logger.info(sb.toString());
        sb = new StringBuffer();
        sb.append("Parms: 1-").append(regionId);
        sb.append(" 2-").append(regionId);
        sb.append(" 3-").append(countyHealthJurisId);
        sb.append(" 4-").append(countyHealthJurisId);
        logger.info(sb.toString());

        rs = stmt.executeQuery();
        while (rs.next()) {
          DupMatchResult obj = PatientHelperDAOOracle.getDupMatchResultResults(rs);
          results.add(obj);
        }

      } catch (SQLException e) {
        e.printStackTrace();
        throw new DAOSysException("SQLException", e);

      } finally {
        CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
        CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
      }
      return results;
    }

  protected static DupMatchResult getDupMatchResultResults(ResultSet rs) 
    throws SQLException {
    DupMatchResult obj = new DupMatchResult();
    obj.setDupMatchResultId(rs.getLong(1));
    long basePatientId = rs.getLong(2);
    obj.setBasePatientId(basePatientId==0?null:basePatientId);
    long matchPatientId = rs.getLong(3);
    obj.setMatchPatientId(matchPatientId==0?null:matchPatientId);
    long priorityTypeId = rs.getLong(4);
    obj.setPriorityTypeId(priorityTypeId==0?null:priorityTypeId);
    obj.setMatchCriteria(rs.getString(5));
    long groupId = rs.getLong(6);
    obj.setGroupId(groupId==0?null:groupId);
    long matchStatusId = rs.getLong(7);
    obj.setMatchStatusId(matchStatusId==0?null:matchStatusId);
    obj.setBaseBirthDate(OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(rs.getString(8),"yyyyMMdd")));
    long basePatientRegionId = rs.getLong(9);
    obj.setBasePatientRegionId(basePatientRegionId==0?null:basePatientRegionId);
    long baseCountyHealthJurisId = rs.getLong(10);
    obj.setBaseCountyHealthJurisId(baseCountyHealthJurisId==0?null:baseCountyHealthJurisId);
    obj.setBasePatientSourceCode(rs.getString(11));
    obj.setMatchBirthDate(OracleDateSupport.getTimestampFromDate(OracleDateSupport.getDateFromString(rs.getString(12),"yyyyMMdd")));
    long matchPatientRegionId = rs.getLong(13);
    obj.setMatchPatientRegionId(matchPatientRegionId==0?null:matchPatientRegionId);
    long matchCountyHealthJurisId = rs.getLong(14);
    obj.setMatchCountyHealthJurisId(matchCountyHealthJurisId==0?null:matchCountyHealthJurisId);
    obj.setMatchPatientSourceCode(rs.getString(15));
    obj.setEbcMergeFlag(rs.getString(16));
    obj.setScore(rs.getDouble(17));
    obj.setStatusMessage(rs.getString(18));
    obj.setDateCreated(rs.getTimestamp(19));
    obj.setCreatedBy(rs.getString(20));
    obj.setDateModified(rs.getTimestamp(21));
    obj.setModifiedBy(rs.getString(22));
    return obj;
  }

  public static PatientRace dbFindPatientRaceByPatientId(Long patientId, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    PatientRace results = null;

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);  
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(PatientRace.SQL_FIND_CHILD_RACE).append(PatientRace.SQL_FILTER_CHILD_ID);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, patientId);

      // debug output prep
      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(patientId);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        results = PatientHelperDAOOracle.getPatientRaceResults(rs);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException thrown: " + e.getMessage());
        
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }
 
  public static PatientEthnicity dbFindPatientEthnicityByPatientId(Long patientId, Connection con)
    throws DAOSysException {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    PatientEthnicity results = null;

    try {
      if (con==null)
        CoreHelperDAOOracle.getDAOInstance().processException(ErrorConstants.ERROR_CONNECTION_NULL, new NullPointerException("Connection object cannot be NULL"));
      logger.debug("using con: " + con);  
      logger.debug("Autocommit state: " + con.getAutoCommit());

      StringBuffer sb = new StringBuffer();
      sb.append(PatientEthnicity.SQL_FIND_CHILD_ETHNICITY).append(PatientRace.SQL_FILTER_CHILD_ID);
      stmt = con.prepareStatement(sb.toString());
      stmt.setLong(1, patientId);

      // debug output prep
      logger.info(sb.toString());
      sb = new StringBuffer();
      sb.append("Parms: 1-").append(patientId);
      logger.info(sb.toString());

      rs = stmt.executeQuery();
      while (rs.next()) {
        results = PatientHelperDAOOracle.getPatientEthnicityResults(rs);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DAOSysException("SQLException thrown: " + e.getMessage());
        
    } finally {
      CoreHelperDAOOracle.getDAOInstance().closeResultSet(rs);
      CoreHelperDAOOracle.getDAOInstance().closeStatement(stmt);
    }
    return results;
  }
  
}

