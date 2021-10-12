//
// $Author$
// $Date$
// $Rev$
// $Id$
//
package gov.mi.mdch.mcir.person.web;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wicket.Component;
import org.apache.wicket.RestartResponseException;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormChoiceComponentUpdatingBehavior;
import org.apache.wicket.ajax.form.AjaxFormComponentUpdatingBehavior;
import org.apache.wicket.behavior.Behavior;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.OnLoadHeaderItem;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.HiddenField;
import org.apache.wicket.markup.html.form.Radio;
import org.apache.wicket.markup.html.form.RadioGroup;
import org.apache.wicket.markup.html.form.TextArea;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.util.convert.IConverter;
import org.publichealthsoftware.base.JmsQueue;

import gov.mi.mdch.mcir.core.CoreConstants;
import gov.mi.mdch.mcir.core.RoleMap;
import gov.mi.mdch.mcir.core.SelectMap;
import gov.mi.mdch.mcir.core.ServiceException;
import gov.mi.mdch.mcir.core.ServiceFactory;
import gov.mi.mdch.mcir.core.persistence.CountyHealthJuris;
import gov.mi.mdch.mcir.core.persistence.Region;
import gov.mi.mdch.mcir.core.web.CalendarField;
import gov.mi.mdch.mcir.core.web.CalendarField.DateFieldBehaviorWrapper;
import gov.mi.mdch.mcir.core.web.DateValidator;
import gov.mi.mdch.mcir.core.web.ErrorPage;
import gov.mi.mdch.mcir.core.web.FlagCheckBox;
import gov.mi.mdch.mcir.core.web.Home;
import gov.mi.mdch.mcir.core.web.KeyPressValidator;
import gov.mi.mdch.mcir.core.web.McirApplication;
import gov.mi.mdch.mcir.core.web.McirForm;
import gov.mi.mdch.mcir.core.web.McirPage;
import gov.mi.mdch.mcir.core.web.ObjectToValueModel;
import gov.mi.mdch.mcir.core.web.PdfReportLink;
import gov.mi.mdch.mcir.core.web.UserSession;
import gov.mi.mdch.mcir.data.BatchVO;
import gov.mi.mdch.mcir.data.Constants;
import gov.mi.mdch.mcir.data.RecallNoticeVO;
import gov.mi.mdch.mcir.data.Select;
import gov.mi.mdch.mcir.data.UserVO;
import gov.mi.mdch.mcir.patient.persistence.Series;
import gov.mi.mdch.mcir.report.McirException;
import gov.mi.mdch.mcir.report.persistence.RequestQueue;
import gov.mi.mdch.mcir.site.SiteException;
import gov.mi.mdch.mcir.site.persistence.SiteClass;
import gov.mi.mdch.mcir.util.Support;

public class CreateRecallNotices extends McirPage {

  private static final Log logger = LogFactory.getLog(CreateRecallNotices.class.getName());
 
  private boolean confirmed;
  private HiddenField<Boolean> confirmation;
  private Button submitButton;
  RecallNoticeVO rN=null;
  DropDownChoice<Select> vacSeries;
  DropDownChoice<Select> vacType;
  CalendarField shotStartDt;
  CalendarField shotEndDt;
  DropDownChoice<CountyHealthJuris> county;
  WebMarkupContainer runTypeContainer;
  FlagCheckBox rosterFlag;
  FlagCheckBox runTypeCd;
  
  public enum Migrant implements Serializable { 
	  EXCLUDE   ("N", "Exclude"),  
	  INCLUDE   ("", "Include"),
	  ONLY      ("Y", "Only");

  public static Migrant findMigrant(String id) {
	  Migrant results = null;
	    for (Migrant obj : Migrant.values()) {
	      if (obj.getKey().equals(id)) results = obj;
	    }
	    if (results==null) {
	      StringBuffer sb = new StringBuffer();
	      sb.append("Migrant not found for id: ").append(id);
	      throw new NullPointerException(sb.toString());
	    }
	    return results;
	  }
	  
	    private final String key;
	    private final String displayName;

	    Migrant(String key, String displayName) {
	      this.key = key;
	      this.displayName = displayName;
	    }

	    public String getKey() {
	      return key;
	    }

	    public String getDisplayName() {
	      return displayName;
	    }

	  }

  private final String RECALL_TEXT   = "If you think your child has received the above missing "
    + "shots please give us a call or if you know that your child has not received the above shots, "
    + "please call for an appointment. Together we can help children be as healthy as possible." ;
  
  UserVO usr; 
  SiteClass siteClass;
  public static final String MONTHS = "1";
  public static final String YEARS = "2";
  RoleMap roleMap;
  String roleId="";
  
  public CreateRecallNotices() {
    super("Recall Outreach Notices", CoreConstants.MODULE_ITEM_RR, false);

    add(new CreateRecallssForm("createRecallsForm"));
  }

  protected class CreateRecallssForm extends McirForm {

    List<Select> vaccineTypes = new ArrayList<Select>();
    List<Select> vaccineSeries = new ArrayList<Select>();
    Collection seriesRecall=null;
	List<CountyHealthJuris> countyList = new ArrayList<CountyHealthJuris>();
	private ConfirmationPopup confirmationPopup = null;

	public CreateRecallssForm(String id) {
      super(id);

      logger.info("rendering " + CreateRecallssForm.class.getName());
      usr = (UserVO) UserSession.get().getHttpSession().getAttribute(UserVO.MY_NAME);
      siteClass= UserSession.get().getSite().getFacilityType().getSiteClass();
      
      seriesRecall = SelectMap.getInstance().seriesRecallSelects();
      for(Iterator i = seriesRecall.iterator(); i.hasNext(); )   {
    	     Select sel = (Select) i.next();
    	     vaccineSeries.add(sel);
      }
      
      HttpSession session = ((HttpServletRequest) RequestCycle.get().getRequest().getContainerRequest()).getSession();
      roleMap = (RoleMap) session.getServletContext().getAttribute(RoleMap.MY_NAME);
      roleId = usr.getRoleId();
      
     final RecallNoticeVO recallNotice = new RecallNoticeVO();
      recallNotice.setDescription("RCL_" + System.currentTimeMillis());
      recallNotice.setCollectionLimit("300");
      recallNotice.setLetterTypeCode(RecallNoticeVO.LETTER_TYPE_COMPREHENSIVE);
      recallNotice.setMigrantFlag("N");
      recallNotice.setFromAge("19");
      recallNotice.setToAge("36");
      recallNotice.setAgeUOM(MONTHS);
      recallNotice.setTargetDate(Support.getCurrentDate());
      recallNotice.setNotifyMethodCode(RecallNoticeVO.NOTIFY_VIA_LETTER);
      recallNotice.setCsvExtractFlag("N");
      if(siteClass.equals(SiteClass.DHHS)){
       recallNotice.setRegion(Region.findRegion(Long.valueOf(usr.getSessionData().getRegionId())));
       recallNotice.setMiFlag("Y");
       recallNotice.setProviderFlag("Y");
      }
      
      if(siteClass.equals(SiteClass.LHD)){
          recallNotice.setRunTypeCode("Y");
          recallNotice.setCounty(CountyHealthJuris.findCountyHealthJuris(Long.valueOf(usr.getSessionData().getCountyHealthJurisdictionId())));
      }
           
      List<Region> listRegions = new ArrayList<Region>();
      for(int i=0; i < 6; i++) { 
    	  listRegions.add(Region.findRegion(Long.valueOf(i+1))); 
      }
        countyList = new ArrayList<CountyHealthJuris>();
	    if (siteClass.equals(SiteClass.RGN) || siteClass.equals(SiteClass.DHHS)) {
	    	setCountyList(CountyHealthJuris.findCountyHealthJurisByRegionId(Long.parseLong(usr.getSite().getRegionId())));
	    } else {
	    	setCountyList(CountyHealthJuris.findCountyHealthJurisByHealthJurisdictionId(Long.parseLong(usr.getSite().getHealthJurisdictionId())));
	    }
	    
      setModel(new CompoundPropertyModel(recallNotice));
      
      add(new FeedbackPanel("feedbackPanel"));

      String defaultText="";
	try {
		defaultText = ServiceFactory.getSiteService3().findNotifyTextBySiteId(UserSession.get().getSite().getSiteId()+"");
		
	} catch (SiteException e) {
		e.printStackTrace();
	} catch (ServiceException e) {
		e.printStackTrace();
	}
	recallNotice.setProviderText(defaultText.length() == 0 ? RECALL_TEXT : defaultText);
      
      if(roleMap.isHCOUser(usr.getRoleId())){
    	  recallNotice.setPatientRosterFlag(Constants.ON);
       }
      
      logger.debug("Recall Notice VO Values: "+recallNotice);
      
      final TextField description = new TextField("description", new PropertyModel(recallNotice, "description")); 
      add(description);
  
      add(newPdfPreviewReportLink(recallNotice));
      
      final RadioGroup letterType = new RadioGroup("p_letter_type_cd", new PropertyModel(recallNotice, "p_letter_type_cd"));
      letterType.setOutputMarkupId(true);
      add(letterType);
      
      letterType.add(new Radio("comprehensive", new Model(RecallNoticeVO.LETTER_TYPE_COMPREHENSIVE)));
      letterType.add(new Radio("simple", new Model(RecallNoticeVO.LETTER_TYPE_SIMPLE)));
      ///letterType.setVisible(false);//Bug# 12677-- To Hide //Bug# 12890 To Reinstate
      
      final TextField leastAge = new TextField("p_from_child_age", new PropertyModel(recallNotice, "p_from_child_age")); 
      add(leastAge);
      
      final TextField maximumAge = new TextField("p_to_child_age", new PropertyModel(recallNotice, "p_to_child_age")); 
      add(maximumAge);
      
      RadioGroup ageGroup = new RadioGroup("age_uom", new PropertyModel(recallNotice, "age_uom"));
      ageGroup.setOutputMarkupId(true);
      add(ageGroup);
      ageGroup.add(new Radio("months", new Model(MONTHS)));
      ageGroup.add(new Radio("years", new Model(YEARS)));

      rosterFlag = new FlagCheckBox("p_roster_fl", new PropertyModel<String>(recallNotice, "p_roster_fl"));
      rosterFlag.setEnabled(!roleMap.isHCOUser(usr.getRoleId()) && !roleMap.isSystemAdministrator(usr.getRoleId()));
      rosterFlag.setOutputMarkupId(true);
      add(rosterFlag);
     
      runTypeContainer = new WebMarkupContainer("runTypeContainer");
      runTypeContainer.setOutputMarkupId(true);
      runTypeContainer.setVisible(roleMap.isLHDProvider(usr.getRoleId()));
      add(runTypeContainer);
      
      runTypeCd = new FlagCheckBox("p_run_type_cd", new PropertyModel<String>(recallNotice, "p_run_type_cd")); 
      runTypeCd.setOutputMarkupId(true);
      runTypeContainer.add(runTypeCd);
      
      final DropDownChoice<Migrant> migrantFl  = new DropDownChoice<Migrant>("p_migrant_fl", new ObjectToValueModel<Migrant, String>() {

          @Override
          protected Migrant convertToObject(String value) {
            if(value !=null && value.length() >0 )
            return Migrant.findMigrant(value);
            else
            return null;
          }

          @Override
          protected String convertToValue(Migrant object) {
            if(object !=null && object.getDisplayName().length() > 0)
              return object.getKey().toString();
            else
              return "";
          }
        }, Arrays.asList(Migrant.values()), new ChoiceRenderer("displayName", "key"));
      
        migrantFl.setNullValid(false);
        add(migrantFl);

      FlagCheckBox notifyMethod = new FlagCheckBox("p_notif_method_cd", new PropertyModel<String>(recallNotice, "p_notif_method_cd")); 
      notifyMethod.setOutputMarkupId(true);
      notifyMethod.setVisible((roleMap.isLHDProvider(roleId)) || (roleMap.isSystemAdministrator(roleId)) || (roleMap.isProvider(roleId)));
      add(notifyMethod);
      
     final WebMarkupContainer dHHSOnlyContainer = new WebMarkupContainer("dHHSOnlyContainer");
      dHHSOnlyContainer.setOutputMarkupId(true);
      dHHSOnlyContainer.setVisible(SiteClass.DHHS.equals(siteClass));
      add(dHHSOnlyContainer);
      
      FlagCheckBox miResidenceOnly = new FlagCheckBox("p_mi_fl", new PropertyModel<String>(recallNotice, "p_mi_fl")); 
      dHHSOnlyContainer.add(miResidenceOnly);

      final FlagCheckBox miProvOnly = new FlagCheckBox("p_prov_fl", new PropertyModel<String>(recallNotice, "p_prov_fl")); 
      dHHSOnlyContainer.add(miProvOnly);
      
      //FlagCheckBox csvExtractFl = new FlagCheckBox("csv_extract_fl", new PropertyModel<String>(recallNotice, "csv_extract_fl")); 
      //add(csvExtractFl);
      
      vacSeries = new DropDownChoice<Select>("p_vac_series", new PropertyModel(this, "vaccineSeries"), new ChoiceRenderer("name", "value"));
      vacSeries.setNullValid(true);
      vacSeries.setOutputMarkupId(true);
      add(vacSeries);
      
     final WebMarkupContainer serAdviceContainer = new WebMarkupContainer("serAdviceContainer") {
          @Override
          public boolean isVisible() {
        	  return vaccineTypes !=null && vaccineTypes.size() > 0;
       	     // String sel = vacSeries.getModelObject()==null?"":vacSeries.getModelObject().getValue();
             // return (sel !=null && !sel.equals("") && (sel.equals("20") || sel.equals("74") || sel.equals("80") || sel.equals("93")));
          }
        };
        serAdviceContainer.setOutputMarkupPlaceholderTag(true);
        add(serAdviceContainer);
        
      vacType = new DropDownChoice<Select>("p_series_advice_id", new PropertyModel(this, "vaccineTypes"), new ChoiceRenderer("name", "value"));
      vacType.setNullValid(true);
      vacType.setOutputMarkupId(true);
      serAdviceContainer.add(vacType);
      
      final TextField doseNumber = new TextField("p_dose_number", new PropertyModel(recallNotice, "p_dose_number")); 
      add(doseNumber);
      
      WebMarkupContainer regionContainer = new WebMarkupContainer("regionContainer") {
          @Override
          public boolean isVisible() {
              return (siteClass.equals(SiteClass.DHHS))    ;
          }
        };
        regionContainer.setOutputMarkupPlaceholderTag(true);
        add(regionContainer);

        
       final DropDownChoice<Region> region = new DropDownChoice<Region>("region", listRegions, new ChoiceRenderer("displayName", "id")){
            @Override
            protected void onBeforeRender() {
              super.onBeforeRender();
            }
          };
          region.setNullValid(false);
          region.setOutputMarkupId(true);
          regionContainer.add(region);
 
       final WebMarkupContainer countyContainer = new WebMarkupContainer("countyContainer") {
            @Override
            public boolean isVisible() {
                //return (siteClass.equals(SiteClass.DHHS) || siteClass.equals(SiteClass.RGN));//Bug#11406
            	return (roleMap.isLHDProvider(roleId)) || (roleMap.isSystemAdministrator(roleId)) ;
            }
          };
          countyContainer.setOutputMarkupPlaceholderTag(true);
          add(countyContainer);
            
       county = new DropDownChoice<CountyHealthJuris>("county", new PropertyModel(this, "countyList") , new ChoiceRenderer("displayName", "id")){
            @Override
            protected void onBeforeRender() {
              super.onBeforeRender();
            }
          };
          county.setNullValid(true);
          county.setOutputMarkupId(true);
          countyContainer.add(county);
          
      final TextField zipCode = new TextField("p_zip_tx", new PropertyModel(recallNotice, "p_zip_tx")); 
      zipCode.add(KeyPressValidator.getNumericInstance());
      add(zipCode);
      
      shotStartDt = new CalendarField("p_shot_start_dt");
      shotStartDt.add(DateFieldBehaviorWrapper.wrap(DateValidator.getDefaultInstance().addValidator(new DateValidator.FutureValidator())));
      shotStartDt.setOutputMarkupId(true);
      add(shotStartDt);

      shotEndDt = new CalendarField("p_shot_end_dt");
      shotEndDt.add(DateFieldBehaviorWrapper.wrap(DateValidator.getDefaultInstance().addValidator(new DateValidator.FutureValidator())));
      shotEndDt.setOutputMarkupId(true);
      add(shotEndDt);
      
      final TextArea<String> providerText =
          new TextArea<String>("providerText", new PropertyModel<String>(recallNotice, "providerText"));
      providerText.setOutputMarkupId(true);
      providerText.add(KeyPressValidator.getGeneralTextInstance());
      add(providerText);
                                    
      final TextField childCount = new TextField("p_child_count", new PropertyModel(recallNotice, "p_child_count")); 
      add(childCount);

      add(confirmation = newConfirmedFlag()); 
      add(submitButton = new Button("submitButton"));
      add(new Link("cancelButton") {
        @Override
        public void onClick() {
          setResponsePage(Home.class);
        }
      });
  
      vacSeries.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
            Select vacSer = vacSeries.getModelObject();
            vacType.clearInput();
            vacType.setModelObject(null); 
            vaccineTypes = new ArrayList<Select>();
            TreeSet<Select>  types=null;
          if(vacSer !=null && vacSer.getValue().length() > 0){  
       	  Series series = Series.findSeriesBySeriesCode(vacSer.getValue());
       	    types = SelectMap.getInstance().vaccineTypeRecallSelects(series.getSeriesId());
   	      if(types !=null && types.size() > 1)
   	        for(Iterator j = types.iterator(); j.hasNext(); )   {
   		   Select select = (Select) j.next();
     		      vaccineTypes.add(select);
            }
          }
            serAdviceContainer.setVisible(types !=null && types.size() >1);
            target.add(serAdviceContainer);
            target.add(vacType);
          }
        });

      vacType.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
              Select vacTypes = vacType.getModelObject();
          }
        });

      runTypeCd.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
        	  if(runTypeCd.getModelObject() !=null) { 
              	if (runTypeCd.getModelObject().toString().equals("true") && roleMap.isLHDProvider(usr.getRoleId())) {
              		rosterFlag.setModelObject(false);
               	   shotStartDt.setEnabled(true) ;
               	   shotEndDt.setEnabled(true) ;
              	} else {
              	   shotStartDt.setEnabled(false) ;
              	   shotEndDt.setEnabled(false) ;
              	}
                  target.add(rosterFlag); 
                  target.add(shotStartDt); 
                  target.add(shotEndDt); 
                }
          }
        });
     
      rosterFlag.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
              if(rosterFlag.getModelObject() !=null) { 
            	if (rosterFlag.getModelObject().toString().equals("true") && runTypeCd.getModelObject() !=null && runTypeCd.getModelObject()== true && !roleMap.isHCOUser(usr.getRoleId())){
            	    runTypeCd.setModelObject(false);
            	    target.add(runTypeContainer); 
                    target.add(runTypeCd);
            	}
              }
          }
        });

      miProvOnly.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
              if(miProvOnly.getModelObject() !=null) { 
            	if (SiteClass.DHHS.equals(siteClass))
            		if(miProvOnly.getModelObject().toString().equals("true"))
            			rN.setProviderFlag("Y");
            		  else
            			rN.setProviderFlag("N");
            	   else
            		   rN.setProviderFlag("N");
            	    target.add(dHHSOnlyContainer); 
                    target.add(miProvOnly);
            	}
              }
        });
      
      letterType.add(new AjaxFormChoiceComponentUpdatingBehavior() {
          protected void onUpdate(AjaxRequestTarget target) {
              
          }
        });
      
      region.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
        	Region curRegion = region.getModelObject();
        	recallNotice.setRegion(curRegion);
            county.clearInput();
            county.setModelObject(null);
            setCountyList(CountyHealthJuris.findCountyHealthJurisByRegionId(curRegion.getRegionId()));
            target.add(countyContainer);
            target.add(county);
          }
        });
  
      county.add(new AjaxFormComponentUpdatingBehavior("change") {
          protected void onUpdate(AjaxRequestTarget target) {
       	  if(county.getModelObject()!=null){
         	 recallNotice.setCounty(CountyHealthJuris.findCountyHealthJuris(county.getModelObject().getCountyHealthJurisId()));
       	  } 
          target.add(countyContainer);
          target.add(county);
          }
        });
      
      providerText.add(new AjaxFormComponentUpdatingBehavior("change") {
          @Override
          protected void onUpdate(AjaxRequestTarget target) {
        	  Object provText = providerText.getModelObject();
        	  if(provText.toString().length() >= 260){
        		target.appendJavaScript("alert('There are " +provText.toString().length()+" characters in the provider text box.The maximum length allowed is 260');");
        	  }
        	  target.add(providerText);
          }
        });
     
      zipCode.add(new AjaxFormComponentUpdatingBehavior("change") {
          @Override
          protected void onUpdate(AjaxRequestTarget target) {
        	  RecallNoticeVO recalVO = (RecallNoticeVO) getModelObject();
        	  if(!recalVO.getZipCode().equals("") && (Integer.parseInt(recalVO.getZipCode())  > 49999 || Integer.parseInt(recalVO.getZipCode()) < 48000))
        		target.appendJavaScript("alert('Enter a valid ZIP Code within the range: 48000 - 49999');");
          }
        });
   
      // Build and add the confirmation popup
      ConfirmationPopup confirm = new ConfirmationPopup();
      add(confirm);
      
    } //Form End

	  
    private HiddenField<Boolean> newConfirmedFlag() {
        HiddenField<Boolean> field = new HiddenField<Boolean>("confirmed", new PropertyModel<Boolean>(CreateRecallNotices.this, "confirmed")) {
          @SuppressWarnings("unchecked")
          @Override
          public final <C> IConverter<C> getConverter(Class<C> type) {
            if (Boolean.class.equals(type)) {
              return (IConverter<C>) new IConverter<Boolean>() {

                public Boolean convertToObject(String value, Locale locale) {
                  return "true".equals(value) ? Boolean.TRUE : Boolean.FALSE;
                }

                public String convertToString(Boolean value, Locale locale) {
                  return value.toString();
                }

              };
            } else {
              return super.getConverter(type);
            }
          }
        };
        field.setType(Boolean.class);
        field.setOutputMarkupId(true);
        return field;
      }
    
    
    @Override
    protected void onSubmitted() {
   
    	rN = (RecallNoticeVO) getModelObject();

        if(!confirmed && confirmationPopup !=null && confirmationPopup.getConfirmationHeader() !=null) {
            add(confirmationPopup.getConfirmationHeader());
            return;
          }

      if(validateForm()) {

        try {
        	String fromAge = rN.getFromAge();
        	String toAge = rN.getToAge();
        	
        	if(!rN.getAgeUOM().equals("") && rN.getAgeUOM().equals("2")){
        		rN.setFromAge(String.valueOf(Integer.parseInt(rN.getFromAge()) * 12));
        		rN.setToAge(String.valueOf(Integer.parseInt(rN.getToAge()) * 12));
        	}

            if(roleMap.isHCOUser(usr.getRoleId())) 
               rN.setPatientRosterFlag(Constants.ON);

            if(rN.getRegion() == null)
               rN.setRegion(Region.findRegion(Long.valueOf(usr.getSessionData().getRegionId())));

            RequestQueue job = new RequestQueue();
            job.setRequestCode(Constants.REPORT_RECALL_OVERDUE);
            job.setJmsQueue(JmsQueue.JOB_QUEUE);
            job.setTargetDateFormatted(rN.getTargetDate());
            job.setUserId(usr.getUserId());
            job.setSiteId(Long.valueOf(usr.getSite().getId()));
            job.setDescription(rN.getDescription());
             
            SimpleDateFormat format = new  SimpleDateFormat("yyyyMMdd");

            // build parameter string
            StringBuffer sb = new StringBuffer();
            sb.append(RecallNoticeVO.AGE_START).append("~").append(rN.getFromAge()).append("^");
            sb.append(RecallNoticeVO.AGE_END).append("~").append(rN.getToAge()).append("^");
            sb.append(RecallNoticeVO.PATIENT_ROSTER_FLAG).append("~").append(rN.getPatientRosterFlag()).append("^");
            sb.append(RecallNoticeVO.CSV_EXTRACT_FLAG).append("~").append(rN.getCsvExtractFlag()).append("^");
            sb.append(RecallNoticeVO.MIGRANT_FLAG).append("~").append(rN.getMigrantFlag()).append("^");
            sb.append(RecallNoticeVO.ZIP_CODE).append("~").append(rN.getZipCode()).append("^");
            sb.append(RecallNoticeVO.SHOT_DATE_START).append("~").append(shotStartDt==null || shotStartDt.getModelObject()==null ? "":format.format(shotStartDt.getModelObject())).append("^");
            sb.append(RecallNoticeVO.SHOT_DATE_END).append("~").append(shotEndDt ==null || shotEndDt.getModelObject()==null ? "":format.format(shotEndDt.getModelObject())).append("^");
            sb.append(RecallNoticeVO.COUNTY_CODE).append("~").append(county.getModelObject()==null ? "":county.getModelObject().getCountyHealthJurisId()).append("^");
            sb.append(RecallNoticeVO.REGION_ID).append("~").append(rN.getRegion().getRegionId()).append("^");
            sb.append(RecallNoticeVO.MI_RESIDENCE_FLAG).append("~").append(rN.getMiFlag()).append("^");
            sb.append(RecallNoticeVO.MI_PROVIDER_FLAG).append("~").append(rN.getProviderFlag()).append("^");
            sb.append(RecallNoticeVO.COLLECTION_LIMIT).append("~").append(rN.getCollectionLimit()).append("^");
            sb.append(RecallNoticeVO.PROVIDER_TEXT).append("~").append(rN.getProviderText()).append("^");
            sb.append(RecallNoticeVO.VACCINE_SERIES).append("~").append(vacSeries.getModelObject()==null ? "":vacSeries.getModelObject().getValue()).append("^");
            sb.append(RecallNoticeVO.VACCINE_TYPE).append("~").append(vacType.getModelObject()==null ? "":vacType.getModelObject().getValue()).append("^");
            sb.append(RecallNoticeVO.DOSE_NUM).append("~").append(rN.getDoseNum()).append("^");
            sb.append(RecallNoticeVO.NOTIFY_METHOD_CODE).append("~").append(rN.getNotifyMethodCode().equals("Y") ? RecallNoticeVO.NOTIFY_VIA_LABEL:RecallNoticeVO.NOTIFY_VIA_LETTER).append("^");
            
            if((runTypeCd.getModelObject() !=null && runTypeCd.getModelObject().toString().equals("true")) || (rosterFlag.getModelObject() !=null && rosterFlag.getModelObject().toString().equals("true")) && !roleMap.isSystemAdministrator(usr.getRoleId())){ // Bug# 11495 provider flag, roaster flag checked, NOT REG OR DCH set run_type_cd='PROV' 
            	sb.append(RecallNoticeVO.RUN_TYPE_CODE).append("~").append(RecallNoticeVO.PROVIDER_BASED).append("^"); 
            } else if(roleMap.isSystemAdministrator(usr.getRoleId()) || roleMap.isLHDProvider(usr.getRoleId())){   // Bug# 11445 REG/DCH/LHD set run_type_cd='POP' else 'PROV'
            	sb.append(RecallNoticeVO.RUN_TYPE_CODE).append("~").append(RecallNoticeVO.POPULATION_BASED).append("^");
            }  else {
            	sb.append(RecallNoticeVO.RUN_TYPE_CODE).append("~").append(RecallNoticeVO.PROVIDER_BASED).append("^");
            }
            //sb.append(RecallNoticeVO.RUN_TYPE_CODE).append("~").append(roleMap.isSystemAdministrator(usr.getRoleId()) ? RecallNoticeVO.POPULATION_BASED : RecallNoticeVO.PROVIDER_BASED).append("^");
            sb.append(RecallNoticeVO.LETTER_TYPE_CODE).append("~").append(rN.getLetterTypeCode()).append("^");
            sb.append(BatchVO.PROTOCOL_ID).append("~").append(usr.getProtocolId()).append("^");
            sb.append(RecallNoticeVO.AGE_UOM).append("~").append(rN.getAgeUOM()).append("^");
            sb.append("p_from_age").append("~").append(fromAge).append("^");
            sb.append("p_to_age").append("~").append(toAge).append("^");
            sb.append("p_dhhs_site").append("~").append(SiteClass.DHHS.equals(siteClass) ?"Y":"N").append("^");
            
            logger.info("Parameter string: " + sb.toString());
            job.setRequestParameterString(sb.toString());
  	    
            String path = McirApplication.get().getServletContext().getRealPath("/");
            logger.debug("Set Context Path: "+(path==null?"":path));
            job.setRealPath(path);
            
            ServiceFactory.getMcirService3().queueJob(job,  UserSession.get().getOracleSession());

            ServiceFactory.getSiteService3().updateNotifyText(usr.getSite().getId(), rN.getProviderText());
            
            setResponsePage(Home.class);
            
        } catch (Exception ex) {
          logger.error(ex.getMessage());
          ex.printStackTrace();
          throw new RestartResponseException(new ErrorPage(ex));
        }

      }
    }

    protected void add(ConfirmationPopup confirmationPopup) {
        this.confirmationPopup = confirmationPopup;
        confirmed = false;
      }
    
    private PdfReportLink newPdfPreviewReportLink(final RecallNoticeVO recallNoticeVO) {
	    PdfReportLink link = new PdfReportLink("previewReport", new PdfReportLink.DynamicPdfResource() {

      @Override
      protected byte[] getPdf() {

        try {
          return ServiceFactory.getMcirService3().getReminderRecallReportPreview("RCO", recallNoticeVO.getProviderText(), recallNoticeVO.getLetterTypeCode().equals("letter_type_comp") ? true:false, UserSession.get().getOracleSession());
        } catch (ServiceException ex) {
          logger.error("ServiceException: " + ex.getMessage());
          ex.printStackTrace();
          throw new RestartResponseException(new ErrorPage(ex));

        } catch (McirException ex) {
          logger.error("McirException: " + ex.getMessage());
              ex.printStackTrace();
              throw new RestartResponseException(new ErrorPage(ex));
            }
          }
        });
    return link;
  }
    
    protected boolean validateForm() {
      boolean isValid = true;

      RecallNoticeVO recalVO = (RecallNoticeVO) getModelObject();

      if(recalVO.getProviderText() != null && recalVO.getProviderText().length() > 260) {
        isValid = false;
        error("There are " +recalVO.getProviderText().length()+" characters in the provider text box.The maximum length allowed is 260");
      }
      
      if(vacSeries.getModelObject() !=null && (vacType.getModelObject() == null && recalVO.getDoseNum().equals(""))){
    	  isValid = false;
          error("Vaccine series specific recalls require a type (if applicable) or dose number");
      }
 
      if(shotStartDt.getModelObject() !=null && shotEndDt.getModelObject() ==null){
    	  isValid = false;
          error("Please enter the dose administered end date.");
      }
    
      if(shotEndDt.getModelObject() !=null && shotStartDt.getModelObject() ==null){
    	  isValid = false;
          error("Please enter the dose administered start date.");
      }
      
      if(shotStartDt.getModelObject() !=null && shotEndDt.getModelObject() !=null  && (shotStartDt.getModelObject()).after(shotEndDt.getModelObject())){
    	  isValid = false;
          error("Dose administered start date should not be greater than dose administered end date");
      }
  
      if(!recalVO.getZipCode().equals("") && (Integer.parseInt(recalVO.getZipCode())  > 49999 || Integer.parseInt(recalVO.getZipCode()) < 48000)){
    	  isValid = false;
          error("Enter a valid ZIP Code within the range: 48000 - 49999");
      }
      
      return isValid;
    }

    public List<Select> getVaccineTypes() {
	    return (vaccineTypes != null) ? vaccineTypes : new ArrayList<Select>();
	  }
    public void setVaccineTypes(List<Select> vaccineTypes) {
	   this.vaccineTypes = vaccineTypes;
	  }
	public List<Select> getVaccineSeries() {
	   return (vaccineSeries != null) ? vaccineSeries : new ArrayList<Select>();
	  }
	public void setVaccineSeries(List<Select> vaccineSeries) {
	   this.vaccineSeries = vaccineSeries;
	  }
    public List<CountyHealthJuris> getCountyList() {
	   return (countyList != null) ? countyList : new ArrayList<CountyHealthJuris>();
	  }
	public void setCountyList(List<CountyHealthJuris> countyList) {
	   this.countyList = countyList;
	  }
  }
 

  /*****************************
  * Confirmation Popup classes
  ******************************/

  protected class ConfirmationPopup implements Serializable {

    public Behavior getConfirmationHeader() {
      return new Behavior() {

        @Override
        public void renderHead(Component c, IHeaderResponse response) {
          StringBuffer sb = new StringBuffer();
          sb.append("if(confirm('").append(getConfirmationMessage()).append("')) {");
          sb.append("  document.getElementById('").append(confirmation.getMarkupId()).append("').value = true;");
          sb.append("  document.getElementById('").append(submitButton.getMarkupId()).append("').click();");
          sb.append("}");
          response.render(OnLoadHeaderItem.forScript(sb.toString()));
        }

        protected String getConfirmationMessage() {
        	
          StringBuffer sb = new StringBuffer();
          sb.append("This is the report criteria you selected.\\n");
          
	      sb.append("* Letter type: "+(rN.getLetterTypeCode().equals(RecallNoticeVO.LETTER_TYPE_COMPREHENSIVE) ?"Comprehensive":"Simple")+"\\n");
	      sb.append("* People at least of age: "+(rN !=null && rN.getFromAge()==null?"":rN.getFromAge()+(rN.getAgeUOM().equals("1") ?" Months":" Years")+"\\n"));
	      sb.append("* People but not yet of age: "+(rN !=null && rN.getToAge()==null?"":rN.getToAge()+(rN.getAgeUOM().equals("1") ?" Months":" Years")+"\\n"));
	    //sb.append("* Exclude Migrants: "+(rN !=null && rN.getMigrantFlag()==null?"":rN.getMigrantFlag().replace("N","Exclude").replace("Y","Only").replace("","Include")+"\\n")); //Bug# 11895
	      sb.append("* Exclude Migrants: "+(rN !=null && rN.getMigrantFlag()==null?"":(rN.getMigrantFlag().equals("N") ?"Exclude":rN.getMigrantFlag().equals("Y")?"Only":rN.getMigrantFlag().equals("")?"Include":"")+"\\n")); 
	      
	    if(SiteClass.DHHS.equals(siteClass) && rN !=null && rN.getMiFlag() !=null)
	      sb.append((rN.getMiFlag().equals("Y")?"* Limit to people ONLY residing in Michigan:  Yes":"* Limit to people ONLY residing in Michigan:  No") +"\\n");

	    if(SiteClass.DHHS.equals(siteClass) && rN !=null && rN.getProviderFlag() !=null)
	      sb.append((rN.getProviderFlag().equals("Y")?"* Limit to people receiving services in Michigan ONLY:  Yes":"* Limit to people receiving services in Michigan ONLY:  No") +"\\n");

	    if(vacSeries.getModelObject() !=null && vacSeries.getModelObject().getName().length() > 0)
	      sb.append("* Limited to vaccine series: "+(vacSeries.getModelObject().getName()+"\\n")); 
	    
	    if(vacType.getModelObject() !=null && vacType.getModelObject().getName().length() > 0)
		  sb.append("* For this type: "+(vacType.getModelObject().getName()+"\\n"));
	    
	    if(rN.getDoseNum() !=null && !rN.getDoseNum().equals(""))
		  sb.append("* For this dose number only: "+(rN.getDoseNum()+"\\n")); 
	    
	    if (siteClass.equals(SiteClass.RGN) || siteClass.equals(SiteClass.DHHS)){
	    	if(county.getModelObject() == null) {
	    	  if(roleMap.isLHDProvider(usr.getRoleId())) 
	    		sb.append("* Limited to all counties within your health district "+"\\n");
	    	  else if(roleMap.isREG(usr.getRoleId())) 
	    		sb.append("* Limited to all counties within your region "+"\\n");
	    	  else 
	    		  sb.append("* NOT Limited to any county "+"\\n");
	    	} else
	    		sb.append("* Within the county of: "+ (county.getModelObject().getDisplayName())+"\\n");
	      }
	    if(rN.getZipCode() !=null && !rN.getZipCode().equals(""))
		  sb.append("* Limited to ZIP Code: "+(rN.getZipCode()+"\\n"));
	    
	    if (siteClass.equals(SiteClass.DHHS))
	      sb.append("* Within region: "+(rN.getRegion().getRegionId()+"\\n"));
	    
	    if(shotStartDt.getModelObject() !=null && shotEndDt.getModelObject() !=null)
	      sb.append("* Doses administered from  "+(Support.getFormattedDate(shotStartDt.getModelObject())) + " up to and including "+(Support.getFormattedDate(shotEndDt.getModelObject()))+"\\n"); 
	    
		if(rN.getCollectionLimit() !=null && !rN.getCollectionLimit().equals(""))
		  sb.append("* Up to this number of people: "+(rN.getCollectionLimit()+"\\n"));
		  
	      sb.append(rN !=null && rN.getNotifyMethodCode()==null?"":(rN.getNotifyMethodCode().equals("Y") ?"* Generate LABELS only: Yes ":"* Generate LETTERS only: Yes")+"\\n");
	      sb.append(rN !=null && rN.getCsvExtractFlag()==null?"":(rN.getCsvExtractFlag().equals("Y") ?"* Generate CSV only: Yes "+"\\n":""));
	      sb.append("* This report will be ready on: "+(rN !=null && rN.getTargetDate()==null?"":rN.getTargetDate()+"\\n"));
	      sb.append("* You named this report: "+(rN !=null && rN.getDescription()==null?"":rN.getDescription()+"\\n\\n"));
	      sb.append("Do you wish to proceed with these criteria?\\n\\n");
	      sb.append("If yes, then select OK;\\n");
	      sb.append("otherwise, press CANCEL to edit.\\n\\n");

            return sb.toString().replace("'", "\\'");
        	
        }

        @Override
        public boolean isTemporary(Component c) {
          return true;
        }
      };
    }
  }
}
