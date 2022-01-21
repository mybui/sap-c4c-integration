from sqlalchemy import create_engine, Integer, Column, String, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

from logging_config import setup_logging
from settings import DB_URI

logger = setup_logging(__name__)

Base = declarative_base()
# set echo="debug" for debug messages
# (a lot, not readable when deployed)
engine = create_engine(url=DB_URI, echo=False)


# define data models
class Contact(Base):
    __tablename__ = "contact"
    C_SFDCContactID = Column(String(100), primary_key=True)
    C_Company = Column(String(100))
    C_SFDCAccountID = Column(String(100))
    C_City = Column(String(100))
    C_Country = Column(String(100))
    C_EmailAddress = Column(String(100))
    C_SAP_Field_of_Work1 = Column(String(100))
    C_FirstName = Column(String(100))
    C_LastName = Column(String(100))
    C_Title = Column(String(100))
    C_MobilePhone = Column(String(100))
    C_SAP_Dealer_ID1 = Column(String(100))
    C_BusPhone = Column(String(100))
    C_SAP___Is_Plannja_Dealer1 = Column(String(100))
    C_SAP_Contact_Status1 = Column(String(100))
    C_Zip_Postal = Column(String(100))
    C_SAP_Plannja_Price_List1 = Column(String(100))
    ContactUUID = Column(String(100))
    # time inserted in UTC
    TimeInsertedUTC = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<Contact(C_SFDCContactID='%s')>" % self.C_SFDCContactID


class Account(Base):
    __tablename__ = "account"
    M_SFDCAccountID = Column(String(100), primary_key=True)
    M_Address1 = Column(String(100))
    M_SAP_BC_ABC_Classification1 = Column(String(100))
    M_City = Column(String(100))
    M_Country = Column(String(100))
    M_CompanyName = Column(String(100))
    M_SAP_Dealer_ID1 = Column(String(100))
    M_SAP_RC_Customer_Segment1 = Column(String(100))
    M_SAP_Account_Role1 = Column(String(100))
    M_SAP_RR_ABC_Classification1 = Column(String(100))
    M_SAP_RR_Tactical_Classification1 = Column(String(100))
    M_SAP_Account_Status1 = Column(String(100))
    M_Zip_Postal = Column(String(100))
    TimeInsertedUTC = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<Account(M_SFDCAccountID='%s')>" % self.M_SFDCAccountID


class Lead(Base):
    __tablename__ = "lead"
    TableID = Column(Integer, primary_key=True, autoincrement=True)
    C_SAP_Lead_ID1 = Column(String(100))
    C_SFDC___Lead_Record_Type_ID1 = Column(String(100))
    C_SFDC___Lead_Name1 = Column(String(100))
    C_SAP_Lead_Status1 = Column(String(100))
    C_SAP_Dealer_ID1 = Column(String(100))
    C_Company = Column(String(100))
    C_Address1 = Column(String(100))
    C_City = Column(String(100))
    C_Zip_Postal = Column(String(100))
    C_State_Prov = Column(String(100))
    C_Country = Column(String(100))
    C_EmailAddress = Column(String(100))
    C_FirstName = Column(String(100))
    C_LastName = Column(String(100))
    C_Title = Column(String(100))
    C_MobilePhone = Column(String(100))
    C_BusPhone = Column(String(100))
    C_Description1 = Column(String(5000))
    C_SAP_Field_of_Work1 = Column(String(100))
    C_SAP_B2B_Product_Group1 = Column(String(100))
    C_SFDC___Dealer_Source1 = Column(String(100))
    C_SAP_Lead_Campaign_Value1 = Column(String(100))
    C_SFDC___Roofing_lead_category1 = Column(String(100))
    C_SAP_Related_Roofing_Installation1 = Column(String(100))
    C_SAP_Related_Roofing_Profile1 = Column(String(100))
    C_SAP_Related_Roofing_RWS1 = Column(String(100))
    C_SAP_Related_Roofing_Safety1 = Column(String(100))
    C_SAP_Related_Roofing_Accessories1 = Column(String(100))
    C_SAP_Related_Roofing_Solar1 = Column(String(100))
    C_SAP_Attachment_I1 = Column(String(300))
    C_SAP_Attachment_II1 = Column(String(300))
    C_SAP_Attachment_III1 = Column(String(300))
    C_SAP_Attachment_IV1 = Column(String(300))
    C_SAP_Attachment_V1 = Column(String(300))
    C_SFDC_iPDF_01_Project_Status1 = Column(String(100))
    C_SFDC_iPDF_13_Billing_Address_Name1 = Column(String(100))
    C_SFDC_iPDF_09_Billing_Address_Street1 = Column(String(100))
    C_SFDC_iPDF_11_Billing_Address_City1 = Column(String(100))
    C_SFDC_iPDF_10_Billing_Address_Zip1 = Column(String(100))
    C_SFDC_iPDF_12_Billing_Address_Country1 = Column(String(100))
    C_SFDC_iPDF_03_Installers_at_Site_Earliest_We = Column(String(100))
    C_SFDC_iPDF_04_Installers_at_Site_Latest_Week = Column(String(100))
    C_SFDC_iPDF_02_Products_at_Site_Week1 = Column(String(100))
    C_SFDC_iPDF_14_Homing_Date1 = Column(String(100))
    C_SFDC_iPDF_05_Project_Manager_1_Name1 = Column(String(100))
    C_SFDC_iPDF_05_Project_Manager_1_Email1 = Column(String(100))
    C_SFDC_iPDF_06_Project_Manager_1_Mobile1 = Column(String(100))
    C_SFDC_iPDF_07_Project_Manager_2_Name_1 = Column(String(100))
    C_SFDC_iPDF_05_Project_Manager_2_Email1 = Column(String(100))
    C_SFDC_iPDF_08_Project_Manager_2_Mobile1 = Column(String(100))
    C_SAP_Created_to_Kata_Date1 = Column(String(100))
    C_SFDC___Created_To_Kata_Time1 = Column(String(100))
    C_Meeting_Date_for_email1 = Column(String(100))
    C_Meeting_time_for_email1 = Column(String(100))
    C_SAP_UTM_Medium_Original1 = Column(String(100))
    C_SAP_UTM_Source_Original1 = Column(String(100))
    C_SAP_UTM_Medium_Recent1 = Column(String(100))
    C_SAP_UTM_Source_Recent1 = Column(String(100))
    C_SAP_Send_Email_to_Dealer1 = Column(String(100))
    C_SFDC_Survey_Status_Installation1 = Column(String(100))
    # marketing permission for b2b
    C_EML_GROUP_Professional1 = Column(String(100))
    # marketing permission for b2c
    C_EML_GROUP_Roofs_and_renovations1 = Column(String(100))
    ContactUUID = Column(String(100))
    URI = Column(String(200))
    TimeInsertedUTC = Column(DateTime(timezone=True))
    # outbound integration
    C4C_Task_Name = Column(String(100))
    C4C_Status = Column(String(100))

    def __repr__(self):
        return "<Lead(C_EmailAddress='%s', C_SAP_Lead_ID1='%s')>" % (self.C_EmailAddress, self.C_SAP_Lead_ID1)


class TargetGroup(Base):
    __tablename__ = "target_group"
    TargetGroupID = Column(String(100), primary_key=True)
    Name = Column(String(100))
    # insert after created in Eloqua
    ContactListIDInEloqua = Column(String(100))
    TimeInsertedUTC = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<TargetGroup(TargetGroupID='%s')>" % self.TargetGroupID


class TargetGroupMember(Base):
    __tablename__ = "target_group_member"
    C_SFDCContactID = Column(String(100))
    TargetGroupID = Column(String(100))
    TimeInsertedUTC = Column(DateTime(timezone=True))
    __table_args__ = (PrimaryKeyConstraint(C_SFDCContactID, TargetGroupID), {})

    def __repr__(self):
        return "<TargetGroupMember(C_SFDCContactID='%s')>" % self.C_SFDCContactID


class MarketingPermission(Base):
    __tablename__ = "marketing_permission"
    ContactUUID = Column(String(100), primary_key=True)
    GeneralConsent = Column(String(100))
    TimeInsertedUTC = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<MarketingPermission(ContactUUID='%s')>" % self.ContactUUID


class Employee(Base):
    __tablename__ = "employee"
    EmployeeID = Column(String(100), primary_key=True)
    Name = Column(String(100))
    Email = Column(String(100))
    Department = Column(String(100))
    Country = Column(String(100))
    BusinessPartnerID = Column(String(100))
    TimeInsertedUTC = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<Employee(EmployeeID='%s')>" % self.EmployeeID


def create_tables():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    logger.debug("Tables Contact, Account, Lead, TargetGroup, TargetGroupMember, MarketingPermission, Employee created")


if __name__ == "__main__":
    create_tables()
