from contextlib import contextmanager
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import delete

from db.tables_init import *
from logging_config import setup_logging

logger = setup_logging(__name__)


# manage psql session
@contextmanager
def start_psql_session():
    session = Session(bind=engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_query(session_, purpose, type):
    '''
    Create Select query object
    :param session_: SQL session
    :param purpose: purpose for query
    :param type: data type matching table name
    :return: Select query object
    '''
    query = None
    if purpose == "eloqua_export":
        if type == "tg":
            query = session_.query(TargetGroup.Name)
    if purpose == "eloqua_import" or purpose == "c4c_import":
        if type == "lead":
            # to be edited
            query = session_.query(Lead.TableID,
                                   Lead.C_SAP_Lead_ID1, Lead.C_SFDC___Lead_Record_Type_ID1, Lead.C_SFDC___Lead_Name1,
                                   Lead.C_SAP_Lead_Status1, Lead.C_SAP_Dealer_ID1,
                                   Lead.C_Company, Lead.C_Address1, Lead.C_City, Lead.C_Zip_Postal, Lead.C_State_Prov,
                                   Lead.C_Country, Lead.C_EmailAddress, Lead.C_FirstName, Lead.C_LastName,
                                   Lead.C_Title, Lead.C_MobilePhone, Lead.C_BusPhone, Lead.C_Description1,
                                   Lead.C_SAP_Field_of_Work1, Lead.C_SAP_B2B_Product_Group1,
                                   Lead.C_SFDC___Dealer_Source1, Lead.C_SAP_Lead_Campaign_Value1,
                                   Lead.C_SFDC___Roofing_lead_category1, Lead.C_SAP_Related_Roofing_Installation1,
                                   Lead.C_SAP_Related_Roofing_Profile1, Lead.C_SAP_Related_Roofing_RWS1,
                                   Lead.C_SAP_Related_Roofing_Safety1, Lead.C_SAP_Related_Roofing_Accessories1,
                                   Lead.C_SAP_Related_Roofing_Solar1, Lead.C_SAP_Attachment_I1,
                                   Lead.C_SAP_Attachment_II1, Lead.C_SAP_Attachment_III1, Lead.C_SAP_Attachment_IV1,
                                   Lead.C_SAP_Attachment_V1, Lead.C_SFDC_iPDF_01_Project_Status1,
                                   Lead.C_SFDC_iPDF_13_Billing_Address_Name1,
                                   Lead.C_SFDC_iPDF_09_Billing_Address_Street1,
                                   Lead.C_SFDC_iPDF_11_Billing_Address_City1,
                                   Lead.C_SFDC_iPDF_10_Billing_Address_Zip1,
                                   Lead.C_SFDC_iPDF_12_Billing_Address_Country1,
                                   Lead.C_SFDC_iPDF_03_Installers_at_Site_Earliest_We,
                                   Lead.C_SFDC_iPDF_04_Installers_at_Site_Latest_Week,
                                   Lead.C_SFDC_iPDF_02_Products_at_Site_Week1, Lead.C_SFDC_iPDF_14_Homing_Date1,
                                   Lead.C_SFDC_iPDF_05_Project_Manager_1_Name1,
                                   Lead.C_SFDC_iPDF_05_Project_Manager_1_Email1,
                                   Lead.C_SFDC_iPDF_06_Project_Manager_1_Mobile1,
                                   Lead.C_SFDC_iPDF_07_Project_Manager_2_Name_1,
                                   Lead.C_SFDC_iPDF_05_Project_Manager_2_Email1,
                                   Lead.C_SFDC_iPDF_08_Project_Manager_2_Mobile1,
                                   Lead.C_SAP_Created_to_Kata_Date1, Lead.C_SFDC___Created_To_Kata_Time1,
                                   Lead.C_Meeting_Date_for_email1, Lead.C_Meeting_time_for_email1,
                                   Lead.C_SAP_UTM_Medium_Original1, Lead.C_SAP_UTM_Source_Original1,
                                   Lead.C_SAP_UTM_Medium_Recent1, Lead.C_SAP_UTM_Source_Recent1,
                                   Lead.ContactUUID, Lead.URI, Lead.C4C_Task_Name, Lead.C4C_Status,
                                   Lead.C_EML_GROUP_Professional1, Lead.C_EML_GROUP_Roofs_and_renovations1,
                                   Lead.C_SFDC_Survey_Status_Installation1)
        if type == "tg":
            query = session_.query(TargetGroup.TargetGroupID, TargetGroup.Name)
        if type == "emp":
            query = session_.query(Employee.Name, Employee.EmployeeID)
    if purpose == "sort_upsert_data":
        if type == "contact":
            query = session_.query(Contact.C_SFDCContactID)
        if type == "account":
            query = session_.query(Account.M_SFDCAccountID)
        if type == "lead":
            query = session_.query(Lead.TableID, Lead.C_SAP_Lead_ID1, Lead.C4C_Task_Name, Lead.C4C_Status)
        if type == "tg":
            query = session_.query(TargetGroup.TargetGroupID)
        if type == "tgm":
            query = session_.query(TargetGroupMember.C_SFDCContactID)
        if type == "mp":
            query = session_.query(MarketingPermission.ContactUUID)
        if type == "emp":
            query = session_.query(Employee.EmployeeID)
    # add other purposes here
    # e.g. if purpose == "..."
    # also, can add cols for outbound integration to Lead query
    # e.g. C4C_Task_Name, C4C_Status, Origin_Eloqua, Origin_C4C
    return query


def get_data_by_time(session_, purpose, type, date_from=None, date_to=None):
    query = create_query(session_=session_, purpose=purpose, type=type)
    result = None
    if query:
        if date_from and date_to:
            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
            if type == "contact":
                result = query.filter(Contact.TimeInsertedUTC >= date_from_dt,
                                      Contact.TimeInsertedUTC <= date_to_dt).all()
            if type == "account":
                result = query.filter(Account.TimeInsertedUTC >= date_from_dt,
                                      Account.TimeInsertedUTC <= date_to_dt).all()
            if type == "lead":
                result = query.filter(Lead.TimeInsertedUTC >= date_from_dt,
                                      Lead.TimeInsertedUTC <= date_to_dt).all()
            if type == "tg":
                result = query.filter(TargetGroup.TimeInsertedUTC >= date_from_dt,
                                      TargetGroup.TimeInsertedUTC <= date_to_dt).all()
            if type == "mp":
                result = query.filter(MarketingPermission.TimeInsertedUTC >= date_from_dt,
                                      MarketingPermission.TimeInsertedUTC <= date_to_dt).all()
            if type == "emp":
                result = query.filter(Employee.TimeInsertedUTC >= date_from_dt,
                                      Employee.TimeInsertedUTC <= date_to_dt).all()
        elif date_from:
            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
            if type == "contact":
                result = query.filter(Contact.TimeInsertedUTC >= date_from_dt).all()
            if type == "account":
                result = query.filter(Account.TimeInsertedUTC >= date_from_dt).all()
            if type == "lead":
                result = query.filter(Lead.TimeInsertedUTC >= date_from_dt).all()
            if type == "tg":
                result = query.filter(TargetGroup.TimeInsertedUTC >= date_from_dt).all()
            if type == "mp":
                result = query.filter(MarketingPermission.TimeInsertedUTC >= date_from_dt).all()
            if type == "emp":
                result = query.filter(Employee.TimeInsertedUTC >= date_from_dt).all()
        elif date_to:
            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
            if type == "contact":
                result = query.filter(Contact.TimeInsertedUTC <= date_to_dt).all()
            if type == "account":
                result = query.filter(Account.TimeInsertedUTC <= date_to_dt).all()
            if type == "lead":
                result = query.filter(Lead.TimeInsertedUTC <= date_to_dt).all()
            if type == "tg":
                result = query.filter(TargetGroup.TimeInsertedUTC <= date_to_dt).all()
            if type == "mp":
                result = query.filter(MarketingPermission.TimeInsertedUTC <= date_to_dt).all()
            if type == "emp":
                result = query.filter(Employee.TimeInsertedUTC <= date_to_dt).all()
        else:
            result = query.all()
    return result


def get_data_for_c4c_import(session_):
    return create_query(session_=session_, purpose="c4c_import", type="lead").filter(Lead.C4C_Status == "pending").all()


def get_data_to_try_again(session_):
    return create_query(session_=session_, purpose="c4c_import", type="lead").filter(Lead.C4C_Status == "processing").all()


def get_data_for_eloqua_import(session_, type):
    result = None
    if type == "emp":
        result = create_query(session_=session_, purpose="eloqua_import", type="emp").all()
    if type == "tg":
        result = create_query(session_=session_, purpose="eloqua_import", type="tg").filter(TargetGroup.ContactListIDInEloqua == None,
                                                                                            TargetGroup.Name != None).all()
    return result


def sort_upsert_data(session_, type, data, outbound=None):
    '''
    Sort data into data to update, data to write and existing data in DB
    :param session_: SQL session
    :param type: data type matching table name
    :param data: raw data to sort
    :return: tuple of: (1) dict of (a) data to update, and (b) data to write, and (2) existing data in DB
    '''
    ids_to_update = []
    result = dict()
    if type == "contact" or type == "tgm":
        ids = [entry_["ContactID"] for entry_ in data]
        existing_data = None
        if type == "contact":
            existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(Contact.C_SFDCContactID.in_(ids)).all()
        if type == "tgm":
            existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(TargetGroupMember.C_SFDCContactID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["ContactID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["ContactID"] not in ids_to_update]
    if type == "account":
        ids = [entry_["AccountID"] for entry_ in data]
        existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(Account.M_SFDCAccountID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["AccountID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["AccountID"] not in ids_to_update]
    if type == "lead":
        if type == "lead":
            if outbound:
                ids = [entry_["TableID"] for entry_ in data]
                existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(
                    Lead.TableID.in_(ids)).all()
                for entry in existing_data:
                    ids_to_update.append(entry[0])
                result["entries_to_update"] = [entry for entry in data if entry["TableID"] in ids_to_update]
                result["entries_to_insert"] = [entry for entry in data if entry["TableID"] not in ids_to_update]
            else:
                ids = [entry_["ID"] for entry_ in data]
                existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(
                    Lead.C_SAP_Lead_ID1.in_(ids)).all()
                for entry in existing_data:
                    ids_to_update.append(entry[1])
                result["entries_to_update"] = [entry for entry in data if entry["ID"] in ids_to_update]
                result["entries_to_insert"] = [entry for entry in data if entry["ID"] not in ids_to_update]
    if type == "tg":
        ids = [entry_["ID"] for entry_ in data]
        existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(TargetGroup.TargetGroupID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["ID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["ID"] not in ids_to_update]
    if type == "mp":
        ids = [entry_["BusinessPartnerUUID"] for entry_ in data]
        existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(MarketingPermission.ContactUUID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["BusinessPartnerUUID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["BusinessPartnerUUID"] not in ids_to_update]
    if type == "emp":
        ids = [entry_["UUID"] for entry_ in data]
        existing_data = create_query(session_=session_, purpose="sort_upsert_data", type=type).filter(Employee.EmployeeID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["UUID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["UUID"] not in ids_to_update]
    return result


def decode_c4c_date_time(date_time_string):
    timestamp = int(date_time_string.split("(")[1].split(")")[0])/1000
    date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
    # format time to be 00:00:00
    time = datetime.fromtimestamp(timestamp).date().strftime("%H:%M:%S")
    return "{0}T{1}".format(date, time)


def upsert_contact(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="contact", data=data)
        contacts_to_update = upsert_data.get("entries_to_update", [])
        contacts_to_insert = upsert_data.get("entries_to_insert", [])
        if contacts_to_update:
            for i in range(0, len(contacts_to_update)):
                session_.execute(
                    Contact.__table__.update()
                        .values
                            (
                                C_Company=contacts_to_update[i].get("AccountUUID", None),
                                C_SFDCAccountID=contacts_to_update[i].get("AccountID", None) or None,
                                C_City=contacts_to_update[i].get("BusinessAddressCity", None) or None,
                                C_Country=contacts_to_update[i].get("BusinessAddressCountryCode", None) or None,
                                C_EmailAddress=contacts_to_update[i].get("Email", None) or None,
                                C_SAP_Field_of_Work1=contacts_to_update[i].get("ZFieldofWork", None) or None,
                                C_FirstName=contacts_to_update[i].get("FirstName", None) or None,
                                C_LastName=contacts_to_update[i].get("LastName", None) or None,
                                C_Title=contacts_to_update[i].get("JobTitle", None) or None,
                                C_MobilePhone=contacts_to_update[i].get("Mobile", None) or None,
                                C_SAP_Dealer_ID1=contacts_to_update[i].get("ContactOwnerUUID", None) or None,
                                C_BusPhone=contacts_to_update[i].get("Phone", None) or None,
                                C_SAP___Is_Plannja_Dealer1=contacts_to_update[i].get("ZisPlannjaDealer", None) or None,
                                C_SAP_Contact_Status1=contacts_to_update[i].get("StatusCode", None) or None,
                                C_Zip_Postal=contacts_to_update[i].get("BusinessAddressStreetPostalCode", None) or None,
                                C_SAP_Plannja_Price_List1=contacts_to_update[i].get("ZPlannjaPricelist", None) or None,
                                ContactUUID=contacts_to_update[i].get("ContactUUID", None) or None,
                                TimeInsertedUTC=datetime.utcnow()
                            )
                        .where(Contact.C_SFDCContactID == contacts_to_update[i]["ContactID"])
                )
            logger.debug("Finish Inbound Bulk Update {0} records to table Contact".format(len(contacts_to_update)))
        if contacts_to_insert:
            session_.execute(
                Contact.__table__.insert(),
                [
                    dict
                        (
                            C_SFDCContactID=contacts_to_insert[i].get("ContactID", None),
                            C_Company=contacts_to_insert[i].get("AccountUUID", None) or None,
                            C_SFDCAccountID=contacts_to_insert[i].get("AccountID", None) or None,
                            C_City=contacts_to_insert[i].get("BusinessAddressCity", None) or None,
                            C_Country=contacts_to_insert[i].get("BusinessAddressCountryCode", None) or None,
                            C_EmailAddress=contacts_to_insert[i].get("Email", None) or None,
                            C_SAP_Field_of_Work1=contacts_to_insert[i].get("ZFieldofWork", None) or None,
                            C_FirstName=contacts_to_insert[i].get("FirstName", None) or None,
                            C_LastName=contacts_to_insert[i].get("LastName", None) or None,
                            C_Title=contacts_to_insert[i].get("JobTitle", None) or None,
                            C_MobilePhone=contacts_to_insert[i].get("Mobile", None) or None,
                            C_SAP_Dealer_ID1=contacts_to_insert[i].get("ContactOwnerUUID", None) or None,
                            C_BusPhone=contacts_to_insert[i].get("Phone", None) or None,
                            C_SAP___Is_Plannja_Dealer1=contacts_to_insert[i].get("ZisPlannjaDealer", None) or None,
                            C_SAP_Contact_Status1=contacts_to_insert[i].get("StatusCode", None) or None,
                            C_Zip_Postal=contacts_to_insert[i].get("BusinessAddressStreetPostalCode", None) or None,
                            C_SAP_Plannja_Price_List1=contacts_to_insert[i].get("ZPlannjaPricelist", None) or None,
                            ContactUUID=contacts_to_insert[i].get("ContactUUID", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(contacts_to_insert))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Contact".format(len(contacts_to_insert)))
    else:
        logger.debug("No data to insert/update table Contact")


def upsert_account(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="account", data=data)
        accounts_to_update = upsert_data.get("entries_to_update", [])
        accounts_to_insert = upsert_data.get("entries_to_insert", [])
        if accounts_to_update:
            for i in range(0, len(accounts_to_update)):
                session_.execute(
                    Account.__table__.update()
                        .values
                            (
                                M_Address1=accounts_to_update[i].get("AddressLine1", None) or None,
                                M_SAP_BC_ABC_Classification1=accounts_to_update[i].get("ZBCABCClassification", None) or None,
                                M_City=accounts_to_update[i].get("City", None) or None,
                                M_Country=accounts_to_update[i].get("CountryCode", None) or None,
                                M_CompanyName=accounts_to_update[i].get("Name", None) or None,
                                M_SAP_Dealer_ID1=accounts_to_update[i].get("OwnerUUID", None) or None,
                                M_SAP_RC_Customer_Segment1=accounts_to_update[i].get("ZRCCustomerSegments_KUT", None) or None,
                                M_SAP_Account_Role1=accounts_to_update[i].get("RoleCode", None) or None,
                                M_SAP_RR_ABC_Classification1=accounts_to_update[i].get("ZRRABCClassification", None) or None,
                                M_SAP_RR_Tactical_Classification1=accounts_to_update[i].get("ZRRTacticalClassification", None) or None,
                                M_SAP_Account_Status1=accounts_to_update[i].get("LifeCycleStatusCode", None) or None,
                                M_Zip_Postal=accounts_to_update[i].get("StreetPostalCode", None) or None,
                                TimeInsertedUTC=datetime.utcnow()
                            )
                        .where(Account.M_SFDCAccountID == accounts_to_update[i]["AccountID"])
                )
            logger.debug("Finish Inbound Bulk Update {0} records to table Account".format(len(accounts_to_update)))
        if accounts_to_insert:
            session_.execute(
                Account.__table__.insert(),
                [
                    dict
                        (
                            M_SFDCAccountID=accounts_to_insert[i].get("AccountID", None),
                            M_Address1=accounts_to_insert[i].get("AddressLine1", None) or None,
                            M_SAP_BC_ABC_Classification1=accounts_to_insert[i].get("ZBCABCClassification", None) or None,
                            M_City=accounts_to_insert[i].get("City", None) or None,
                            M_Country=accounts_to_insert[i].get("CountryCode", None) or None,
                            M_CompanyName=accounts_to_insert[i].get("Name", None) or None,
                            M_SAP_Dealer_ID1=accounts_to_insert[i].get("OwnerUUID", None) or None,
                            M_SAP_RC_Customer_Segment1=accounts_to_insert[i].get("ZRCCustomerSegments_KUT", None) or None,
                            M_SAP_Account_Role1=accounts_to_insert[i].get("RoleCode", None) or None,
                            M_SAP_RR_ABC_Classification1=accounts_to_insert[i].get("ZRRABCClassification", None) or None,
                            M_SAP_RR_Tactical_Classification1=accounts_to_insert[i].get("ZRRTacticalClassification", None) or None,
                            M_SAP_Account_Status1=accounts_to_insert[i].get("LifeCycleStatusCode", None) or None,
                            M_Zip_Postal=accounts_to_insert[i].get("StreetPostalCode", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(accounts_to_insert))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Account".format(len(accounts_to_insert)))
    else:
        logger.debug("No data to insert/update table Account")


def upsert_lead(session_, data, inbound=None, outbound=None):
    if data:
        if inbound:
            upsert_data = sort_upsert_data(session_=session_, type="lead", data=data)
            leads_to_update = upsert_data.get("entries_to_update", [])
            leads_to_insert = upsert_data.get("entries_to_insert", [])
            if leads_to_update:
                for i in range(0, len(leads_to_update)):
                    session_.execute(
                        Lead.__table__.update()
                            .values
                                (
                                    C_SFDC___Lead_Record_Type_ID1=leads_to_update[i].get("GroupCode", None) or None,
                                    C_SFDC___Lead_Name1=leads_to_update[i].get("Name", None) or None,
                                    C_SAP_Lead_Status1=leads_to_update[i].get("UserStatusCode", None) or None,
                                    C_SAP_Dealer_ID1=leads_to_update[i].get("OwnerPartyUUID", None) or None,
                                    C_Company=leads_to_update[i].get("Company", None) or None,
                                    C_Address1=leads_to_update[i].get("AccountPostalAddressElementsStreetName", None) or None,
                                    C_City=leads_to_update[i].get("AccountCity", None) or None,
                                    C_Zip_Postal=leads_to_update[i].get("AccountPostalAddressElementsStreetPostalCode", None) or None,
                                    C_State_Prov=leads_to_update[i].get("AccountState", None) or None,
                                    C_Country=leads_to_update[i].get("AccountCountry", None) or None,
                                    C_EmailAddress=leads_to_update[i]["ZConsumerEMail_KUT"] or None
                                        if leads_to_update[i].get("ZConsumerEMail_KUT", None)
                                        else leads_to_update[i].get("ContactEMail", None) or None,
                                    C_FirstName=leads_to_update[i].get("ContactFirstName", None) or None,
                                    C_LastName=leads_to_update[i].get("ContactLastName", None) or None,
                                    C_Title=leads_to_update[i].get("ContactFunctionalTitleName", None) or None,
                                    C_MobilePhone=leads_to_update[i].get("ContactMobile", None) or None,
                                    C_BusPhone=leads_to_update[i].get("ContactPhone", None) or None,
                                    C_Description1=leads_to_update[i].get("Note", None) or None,
                                    C_SAP_Field_of_Work1=leads_to_update[i].get("ZFieldofWork_KUT", None) or None,
                                    C_SAP_B2B_Product_Group1=leads_to_update[i].get("ZProductGrp_KUT", None) or None,
                                    C_SFDC___Dealer_Source1=leads_to_update[i].get("ZDealerSource_KUT", None) or None,
                                    C_SAP_Lead_Campaign_Value1=leads_to_update[i].get("ZCampaignvalue_KUT", None) or None,
                                    C_SFDC___Roofing_lead_category1=leads_to_update[i].get("ZRoofLeadCat_KUT", None) or None,
                                    C_SAP_Related_Roofing_Installation1=leads_to_update[i].get("ZRelRoofInstallation_KUT", None) or None,
                                    C_SAP_Related_Roofing_Profile1=leads_to_update[i].get("ZRelRoofProfile_KUT", None) or None,
                                    C_SAP_Related_Roofing_RWS1=leads_to_update[i].get("ZRelRoofRWS_KUT", None) or None,
                                    C_SAP_Related_Roofing_Safety1=leads_to_update[i].get("ZRelRoofSafety_KUT", None) or None,
                                    C_SAP_Related_Roofing_Accessories1=leads_to_update[i].get("ZRelRoofAccessories_KUT", None) or None,
                                    C_SAP_Related_Roofing_Solar1=leads_to_update[i].get("ZRelRoofSolar_KUT", None) or None,
                                    C_SAP_Attachment_I1=leads_to_update[i].get("ZAttachment1URL_KUT", None) or None,
                                    C_SAP_Attachment_II1=leads_to_update[i].get("ZAttachment2URL_KUT", None) or None,
                                    C_SAP_Attachment_III1=leads_to_update[i].get("ZAttachment3URL_KUT", None) or None,
                                    C_SAP_Attachment_IV1=leads_to_update[i].get("ZAttachment4URL_KUT", None) or None,
                                    C_SAP_Attachment_V1=leads_to_update[i].get("ZAttachment5URL_KUT", None) or None,
                                    C_SFDC_iPDF_01_Project_Status1=leads_to_update[i].get("ZProjectStatus_KUT", None) or None,
                                    C_SFDC_iPDF_13_Billing_Address_Name1=leads_to_update[i].get("ZBillAddrName_KUT", None) or None,
                                    C_SFDC_iPDF_09_Billing_Address_Street1=leads_to_update[i].get("ZBillAddrStreet_KUT", None) or None,
                                    C_SFDC_iPDF_11_Billing_Address_City1=leads_to_update[i].get("ZBillAddrCity_KUT", None) or None,
                                    C_SFDC_iPDF_10_Billing_Address_Zip1=leads_to_update[i].get("ZBillAddrPostcode_KUT", None) or None,
                                    C_SFDC_iPDF_12_Billing_Address_Country1=leads_to_update[i].get("ZBillAddrCtry_KUT", None) or None,
                                    C_SFDC_iPDF_03_Installers_at_Site_Earliest_We=leads_to_update[i].get("ZInstatsiteearlatweek_KUT", None) or None,
                                    C_SFDC_iPDF_04_Installers_at_Site_Latest_Week=leads_to_update[i].get("ZInstatsitelateatweek_KUT", None) or None,
                                    C_SFDC_iPDF_02_Products_at_Site_Week1=leads_to_update[i].get("ZProductsatsiteweek_KUT", None) or None,
                                    C_SFDC_iPDF_14_Homing_Date1=decode_c4c_date_time(leads_to_update[i].get("ZEloquaHomLettDate_KUT", None))
                                        if leads_to_update[i].get("ZEloquaHomLettDate_KUT", None)
                                        else None,
                                    C_SFDC_iPDF_05_Project_Manager_1_Name1=leads_to_update[i].get("ZProjMan1Name_KUT", None) or None,
                                    C_SFDC_iPDF_05_Project_Manager_1_Email1=leads_to_update[i].get("ZProjMan1Email_KUT", None) or None,
                                    C_SFDC_iPDF_06_Project_Manager_1_Mobile1=leads_to_update[i].get("ZProjMan1Mobile_KUT", None) or None,
                                    C_SFDC_iPDF_07_Project_Manager_2_Name_1=leads_to_update[i].get("ZProjMan2Name_KUT", None) or None,
                                    C_SFDC_iPDF_05_Project_Manager_2_Email1=leads_to_update[i].get("ZProjMan2Email_KUT", None) or None,
                                    C_SFDC_iPDF_08_Project_Manager_2_Mobile1=leads_to_update[i].get("ZProjMan2Mobile_KUT", None) or None,
                                    C_SAP_Created_to_Kata_Date1=decode_c4c_date_time(leads_to_update[i].get("ZCreatedToKataDate_KUT", None))
                                        if leads_to_update[i].get("ZCreatedToKataDate_KUT", None)
                                        else None,
                                    C_SFDC___Created_To_Kata_Time1=leads_to_update[i].get("ZCreatedToKataTime_KUT", None) or None,
                                    C_Meeting_Date_for_email1=decode_c4c_date_time(leads_to_update[i].get("ZAgreedappdate_KUT", None))
                                        if leads_to_update[i].get("ZAgreedappdate_KUT", None)
                                        else None,
                                    C_Meeting_time_for_email1=leads_to_update[i].get("ZAgreedapptime_KUT", None) or None,
                                    C_SAP_UTM_Medium_Original1=leads_to_update[i].get("ZUTMMediumOriginal_KUT", None) or None,
                                    C_SAP_UTM_Source_Original1=leads_to_update[i].get("ZUTMSourceOriginal_KUT", None) or None,
                                    C_SAP_UTM_Medium_Recent1=leads_to_update[i].get("ZUTMMediumRecent_KUT", None) or None,
                                    C_SAP_UTM_Source_Recent1=leads_to_update[i].get("ZUTMSourceRecent_KUT", None) or None,
                                    C_SAP_Send_Email_to_Dealer1=leads_to_update[i].get("ZSendEmailToDealer_KUT", None) or None,
                                    C_SFDC_Survey_Status_Installation1=leads_to_update[i].get("ZCustomerSurveyStatus_KUT", None) or None,
                                    ContactUUID=leads_to_update[i].get("ContactUUID", None) or None,
                                    URI=leads_to_update[i].get("__metadata", None).get("uri", None) or None,
                                    TimeInsertedUTC=datetime.utcnow(),
                                )
                            .where(Lead.C_SAP_Lead_ID1 == leads_to_update[i]["ID"])
                    )
                logger.debug("Finish Inbound Bulk Update {0} records to table Lead".format(len(leads_to_update)))
            if leads_to_insert:
                session_.execute(
                    Lead.__table__.insert(),
                    [
                        dict
                            (
                                C_SAP_Lead_ID1=leads_to_insert[i].get("ID", None),
                                C_SFDC___Lead_Record_Type_ID1=leads_to_insert[i].get("GroupCode", None) or None,
                                C_SFDC___Lead_Name1=leads_to_insert[i].get("Name", None) or None,
                                C_SAP_Lead_Status1=leads_to_insert[i].get("UserStatusCode", None) or None,
                                C_SAP_Dealer_ID1=leads_to_insert[i].get("OwnerPartyUUID", None) or None,
                                C_Company=leads_to_insert[i].get("Company", None) or None,
                                C_Address1=leads_to_insert[i].get("AccountPostalAddressElementsStreetName", None) or None,
                                C_City=leads_to_insert[i].get("AccountCity", None) or None,
                                C_Zip_Postal=leads_to_insert[i].get("AccountPostalAddressElementsStreetPostalCode", None) or None,
                                C_State_Prov=leads_to_insert[i].get("AccountState", None) or None,
                                C_Country=leads_to_insert[i].get("AccountCountry", None) or None,
                                C_EmailAddress=leads_to_insert[i]["ZConsumerEMail_KUT"] or None
                                    if leads_to_insert[i].get("ZConsumerEMail_KUT", None)
                                    else leads_to_insert[i].get("ContactEMail", None) or None,
                                C_FirstName=leads_to_insert[i].get("ContactFirstName", None) or None,
                                C_LastName=leads_to_insert[i].get("ContactLastName", None) or None,
                                C_Title=leads_to_insert[i].get("ContactFunctionalTitleName", None) or None,
                                C_MobilePhone=leads_to_insert[i].get("ContactMobile", None) or None,
                                C_BusPhone=leads_to_insert[i].get("ContactPhone", None) or None,
                                C_Description1=leads_to_insert[i].get("Note", None) or None,
                                C_SAP_Field_of_Work1=leads_to_insert[i].get("ZFieldofWork_KUT", None) or None,
                                C_SAP_B2B_Product_Group1=leads_to_insert[i].get("ZProductGrp_KUT", None) or None,
                                C_SFDC___Dealer_Source1=leads_to_insert[i].get("ZDealerSource_KUT", None) or None,
                                C_SAP_Lead_Campaign_Value1=leads_to_insert[i].get("ZCampaignvalue_KUT", None) or None,
                                C_SFDC___Roofing_lead_category1=leads_to_insert[i].get("ZRoofLeadCat_KUT", None) or None,
                                C_SAP_Related_Roofing_Installation1=leads_to_insert[i].get("ZRelRoofInstallation_KUT", None) or None,
                                C_SAP_Related_Roofing_Profile1=leads_to_insert[i].get("ZRelRoofProfile_KUT", None) or None,
                                C_SAP_Related_Roofing_RWS1=leads_to_insert[i].get("ZRelRoofRWS_KUT", None) or None,
                                C_SAP_Related_Roofing_Safety1=leads_to_insert[i].get("ZRelRoofSafety_KUT", None) or None,
                                C_SAP_Related_Roofing_Accessories1=leads_to_insert[i].get("ZRelRoofAccessories_KUT", None) or None,
                                C_SAP_Related_Roofing_Solar1=leads_to_insert[i].get("ZRelRoofSolar_KUT", None) or None,
                                C_SAP_Attachment_I1=leads_to_insert[i].get("ZAttachment1URL_KUT", None) or None,
                                C_SAP_Attachment_II1=leads_to_insert[i].get("ZAttachment2URL_KUT", None) or None,
                                C_SAP_Attachment_III1=leads_to_insert[i].get("ZAttachment3URL_KUT", None) or None,
                                C_SAP_Attachment_IV1=leads_to_insert[i].get("ZAttachment4URL_KUT", None) or None,
                                C_SAP_Attachment_V1=leads_to_insert[i].get("ZAttachment5URL_KUT", None) or None,
                                C_SFDC_iPDF_01_Project_Status1=leads_to_insert[i].get("ZProjectStatus_KUT", None) or None,
                                C_SFDC_iPDF_13_Billing_Address_Name1=leads_to_insert[i].get("ZBillAddrName_KUT", None) or None,
                                C_SFDC_iPDF_09_Billing_Address_Street1=leads_to_insert[i].get("ZBillAddrStreet_KUT", None) or None,
                                C_SFDC_iPDF_11_Billing_Address_City1=leads_to_insert[i].get("ZBillAddrCity_KUT", None) or None,
                                C_SFDC_iPDF_10_Billing_Address_Zip1=leads_to_insert[i].get("ZBillAddrPostcode_KUT", None) or None,
                                C_SFDC_iPDF_12_Billing_Address_Country1=leads_to_insert[i].get("ZBillAddrCtry_KUT", None) or None,
                                C_SFDC_iPDF_03_Installers_at_Site_Earliest_We=leads_to_insert[i].get("ZInstatsiteearlatweek_KUT", None) or None,
                                C_SFDC_iPDF_04_Installers_at_Site_Latest_Week=leads_to_insert[i].get("ZInstatsitelateatweek_KUT", None) or None,
                                C_SFDC_iPDF_02_Products_at_Site_Week1=leads_to_insert[i].get("ZProductsatsiteweek_KUT", None) or None,
                                C_SFDC_iPDF_14_Homing_Date1=decode_c4c_date_time(leads_to_insert[i].get("ZEloquaHomLettDate_KUT", None))
                                    if leads_to_insert[i].get("ZEloquaHomLettDate_KUT", None)
                                    else None,
                                C_SFDC_iPDF_05_Project_Manager_1_Name1=leads_to_insert[i].get("ZProjMan1Name_KUT", None) or None,
                                C_SFDC_iPDF_05_Project_Manager_1_Email1=leads_to_insert[i].get("ZProjMan1Email_KUT", None) or None,
                                C_SFDC_iPDF_06_Project_Manager_1_Mobile1=leads_to_insert[i].get("ZProjMan1Mobile_KUT", None) or None,
                                C_SFDC_iPDF_07_Project_Manager_2_Name_1=leads_to_insert[i].get("ZProjMan2Name_KUT", None) or None,
                                C_SFDC_iPDF_05_Project_Manager_2_Email1=leads_to_insert[i].get("ZProjMan2Email_KUT", None) or None,
                                C_SFDC_iPDF_08_Project_Manager_2_Mobile1=leads_to_insert[i].get("ZProjMan2Mobile_KUT", None) or None,
                                C_SAP_Created_to_Kata_Date1=decode_c4c_date_time(leads_to_insert[i].get("ZCreatedToKataDate_KUT", None))
                                    if leads_to_insert[i].get("ZCreatedToKataDate_KUT", None)
                                    else None,
                                C_SFDC___Created_To_Kata_Time1=leads_to_insert[i].get("ZCreatedToKataTime_KUT", None) or None,
                                C_Meeting_Date_for_email1=decode_c4c_date_time(leads_to_insert[i].get("ZAgreedappdate_KUT", None))
                                    if leads_to_insert[i].get("ZAgreedappdate_KUT", None)
                                    else None,
                                C_Meeting_time_for_email1=leads_to_insert[i].get("ZAgreedapptime_KUT", None) or None,
                                C_SAP_UTM_Medium_Original1=leads_to_insert[i].get("ZUTMMediumOriginal_KUT", None) or None,
                                C_SAP_UTM_Source_Original1=leads_to_insert[i].get("ZUTMSourceOriginal_KUT", None) or None,
                                C_SAP_UTM_Medium_Recent1=leads_to_insert[i].get("ZUTMMediumRecent_KUT", None) or None,
                                C_SAP_UTM_Source_Recent1=leads_to_insert[i].get("ZUTMSourceRecent_KUT", None) or None,
                                C_SAP_Send_Email_to_Dealer1=leads_to_insert[i].get("ZSendEmailToDealer_KUT", None) or None,
                                C_SFDC_Survey_Status_Installation1=leads_to_insert[i].get("ZCustomerSurveyStatus_KUT", None) or None,
                                ContactUUID=leads_to_insert[i].get("ContactUUID", None) or None,
                                URI=leads_to_insert[i].get("__metadata", None).get("uri", None) or None,
                                TimeInsertedUTC=datetime.utcnow(),
                            )
                        for i in range(0, len(leads_to_insert))
                    ]
                )
                logger.debug("Finish Inbound Bulk Insert {0} records table Lead".format(len(leads_to_insert)))

        if outbound:
            upsert_data = sort_upsert_data(session_=session_, type="lead", data=data, outbound=outbound)
            leads_to_update = upsert_data.get("entries_to_update", [])
            leads_to_insert = upsert_data.get("entries_to_insert", [])
            if leads_to_update:
                for i in range(0, len(leads_to_update)):
                    session_.execute(
                        Lead.__table__.update()
                            .values
                                (
                                    C_SAP_Lead_ID1=leads_to_update[i].get("ID", None) or None,
                                    C_SFDC___Lead_Record_Type_ID1=leads_to_update[i].get("GroupCode", None) or None,
                                    C_SFDC___Lead_Name1=leads_to_update[i].get("Name", None) or None,
                                    C_SAP_Lead_Status1=leads_to_update[i].get("UserStatusCode", None) or None,
                                    C_SAP_Dealer_ID1=leads_to_update[i].get("OwnerPartyUUID", None) or None,
                                    C_Company=leads_to_update[i].get("Company", None) or None,
                                    C_Address1=leads_to_update[i].get("AccountPostalAddressElementsStreetName", None) or None,
                                    C_City=leads_to_update[i].get("AccountCity", None) or None,
                                    C_Zip_Postal=leads_to_update[i].get("AccountPostalAddressElementsStreetPostalCode", None) or None,
                                    C_State_Prov=leads_to_update[i].get("AccountState", None) or None,
                                    C_Country=leads_to_update[i].get("AccountCountry", None) or None,
                                    C_EmailAddress=leads_to_update[i].get("Email", None) or None,
                                    C_FirstName=leads_to_update[i].get("ContactFirstName", None) or None,
                                    C_LastName=leads_to_update[i].get("ContactLastName", None) or None,
                                    C_Title=leads_to_update[i].get("ContactFunctionalTitleName", None) or None,
                                    C_MobilePhone=leads_to_update[i].get("ContactMobile", None) or None,
                                    C_BusPhone=leads_to_update[i].get("ContactPhone", None) or None,
                                    C_Description1=leads_to_update[i].get("Note", None) or None,
                                    C_SAP_Field_of_Work1=leads_to_update[i].get("ZFieldofWork_KUT", None) or None,
                                    C_SAP_B2B_Product_Group1=leads_to_update[i].get("ZProductGrp_KUT", None) or None,
                                    C_SFDC___Dealer_Source1=leads_to_update[i].get("ZDealerSource_KUT", None) or None,
                                    C_SAP_Lead_Campaign_Value1=leads_to_update[i].get("ZCampaignvalue_KUT", None) or None,
                                    C_SFDC___Roofing_lead_category1=leads_to_update[i].get("ZRoofLeadCat_KUT", None) or None,
                                    C_SAP_Related_Roofing_Installation1=leads_to_update[i].get("ZRelRoofInstallation_KUT", None) or None,
                                    C_SAP_Related_Roofing_Profile1=leads_to_update[i].get("ZRelRoofProfile_KUT", None) or None,
                                    C_SAP_Related_Roofing_RWS1=leads_to_update[i].get("ZRelRoofRWS_KUT", None) or None,
                                    C_SAP_Related_Roofing_Safety1=leads_to_update[i].get("ZRelRoofSafety_KUT", None) or None,
                                    C_SAP_Related_Roofing_Accessories1=leads_to_update[i].get("ZRelRoofAccessories_KUT", None) or None,
                                    C_SAP_Related_Roofing_Solar1=leads_to_update[i].get("ZRelRoofSolar_KUT", None) or None,
                                    C_SAP_Attachment_I1=leads_to_update[i].get("ZAttachment1URL_KUT", None) or None,
                                    C_SAP_Attachment_II1=leads_to_update[i].get("ZAttachment2URL_KUT", None) or None,
                                    C_SAP_Attachment_III1=leads_to_update[i].get("ZAttachment3URL_KUT", None) or None,
                                    C_SAP_Attachment_IV1=leads_to_update[i].get("ZAttachment4URL_KUT", None) or None,
                                    C_SAP_Attachment_V1=leads_to_update[i].get("ZAttachment5URL_KUT", None) or None,
                                    C_SFDC_iPDF_01_Project_Status1=leads_to_update[i].get("ZProjectStatus_KUT", None) or None,
                                    C_SFDC_iPDF_13_Billing_Address_Name1=leads_to_update[i].get("ZBillAddrName_KUT", None) or None,
                                    C_SFDC_iPDF_09_Billing_Address_Street1=leads_to_update[i].get("ZBillAddrStreet_KUT", None) or None,
                                    C_SFDC_iPDF_11_Billing_Address_City1=leads_to_update[i].get("ZBillAddrCity_KUT", None) or None,
                                    C_SFDC_iPDF_10_Billing_Address_Zip1=leads_to_update[i].get("ZBillAddrPostcode_KUT", None) or None,
                                    C_SFDC_iPDF_12_Billing_Address_Country1=leads_to_update[i].get("ZBillAddrCtry_KUT", None) or None,
                                    C_SFDC_iPDF_03_Installers_at_Site_Earliest_We=leads_to_update[i].get("ZInstatsiteearlatweek_KUT", None) or None,
                                    C_SFDC_iPDF_04_Installers_at_Site_Latest_Week=leads_to_update[i].get("ZInstatsitelateatweek_KUT", None) or None,
                                    C_SFDC_iPDF_02_Products_at_Site_Week1=leads_to_update[i].get("ZProductsatsiteweek_KUT", None) or None,
                                    C_SFDC_iPDF_14_Homing_Date1=leads_to_update[i].get("ZEloquaHomLettDate_KUT", None) or None,
                                    C_SFDC_iPDF_05_Project_Manager_1_Name1=leads_to_update[i].get("ZProjMan1Name_KUT", None) or None,
                                    C_SFDC_iPDF_05_Project_Manager_1_Email1=leads_to_update[i].get("ZProjMan1Email_KUT", None) or None,
                                    C_SFDC_iPDF_06_Project_Manager_1_Mobile1=leads_to_update[i].get("ZProjMan1Mobile_KUT", None) or None,
                                    C_SFDC_iPDF_07_Project_Manager_2_Name_1=leads_to_update[i].get("ZProjMan2Name_KUT", None) or None,
                                    C_SFDC_iPDF_05_Project_Manager_2_Email1=leads_to_update[i].get("ZProjMan2Email_KUT", None) or None,
                                    C_SFDC_iPDF_08_Project_Manager_2_Mobile1=leads_to_update[i].get("ZProjMan2Mobile_KUT", None) or None,
                                    C_SAP_Created_to_Kata_Date1=leads_to_update[i].get("ZCreatedToKataDate_KUT", None) or None,
                                    C_SFDC___Created_To_Kata_Time1=leads_to_update[i].get("ZCreatedToKataTime_KUT", None) or None,
                                    C_Meeting_Date_for_email1=leads_to_update[i].get("ZAgreedappdate_KUT", None) or None,
                                    C_Meeting_time_for_email1=leads_to_update[i].get("ZAgreedapptime_KUT", None) or None,
                                    C_SAP_UTM_Medium_Original1=leads_to_update[i].get("ZUTMMediumOriginal_KUT", None) or None,
                                    C_SAP_UTM_Source_Original1=leads_to_update[i].get("ZUTMSourceOriginal_KUT", None) or None,
                                    C_SAP_UTM_Medium_Recent1=leads_to_update[i].get("ZUTMMediumRecent_KUT", None) or None,
                                    C_SAP_UTM_Source_Recent1=leads_to_update[i].get("ZUTMSourceRecent_KUT", None) or None,
                                    C_EML_GROUP_Professional1=leads_to_update[i].get("B2B_MP_Consent", None) or None,
                                    C_EML_GROUP_Roofs_and_renovations1 = leads_to_update[i].get("B2C_MP_Consent", None) or None,
                                    C_SFDC_Survey_Status_Installation1=leads_to_update[i].get("ZCustomerSurveyStatus_KUT", None) or None,
                                    ContactUUID=leads_to_update[i].get("ContactUUID", None) or None,
                                    URI=leads_to_update[i].get("URI", None) or None,
                                    C4C_Task_Name=leads_to_update[i].get("C4C_Task_Name", None) or None,
                                    C4C_Status=leads_to_update[i].get("C4C_Status", None) or None,
                                    TimeInsertedUTC=datetime.utcnow(),
                                )
                            .where(Lead.TableID == leads_to_update[i]["TableID"])
                    )
                logger.debug("Finish Outbound Bulk Update {0} records to table Lead".format(len(leads_to_update)))
            if leads_to_insert:
                session_.execute(
                    Lead.__table__.insert(),
                    [
                        dict
                            (
                                C_SAP_Lead_ID1=leads_to_insert[i].get("ID", None) or None,
                                C_SFDC___Lead_Record_Type_ID1=leads_to_insert[i].get("GroupCode", None) or None,
                                C_SFDC___Lead_Name1=leads_to_insert[i].get("Name", None) or None,
                                C_SAP_Lead_Status1=leads_to_insert[i].get("UserStatusCode", None) or None,
                                C_SAP_Dealer_ID1=leads_to_insert[i].get("OwnerPartyUUID", None) or None,
                                C_Company=leads_to_insert[i].get("Company", None) or None,
                                C_Address1=leads_to_insert[i].get("AccountPostalAddressElementsStreetName", None) or None,
                                C_City=leads_to_insert[i].get("AccountCity", None) or None,
                                C_Zip_Postal=leads_to_insert[i].get("AccountPostalAddressElementsStreetPostalCode", None) or None,
                                C_State_Prov=leads_to_insert[i].get("AccountState", None) or None,
                                C_Country=leads_to_insert[i].get("AccountCountry", None) or None,
                                C_EmailAddress=leads_to_insert[i].get("Email", None) or None,
                                C_FirstName=leads_to_insert[i].get("ContactFirstName", None) or None,
                                C_LastName=leads_to_insert[i].get("ContactLastName", None) or None,
                                C_Title=leads_to_insert[i].get("ContactFunctionalTitleName", None) or None,
                                C_MobilePhone=leads_to_insert[i].get("ContactMobile", None) or None,
                                C_BusPhone=leads_to_insert[i].get("ContactPhone", None) or None,
                                C_Description1=leads_to_insert[i].get("Note", None) or None,
                                C_SAP_Field_of_Work1=leads_to_insert[i].get("ZFieldofWork_KUT", None) or None,
                                C_SAP_B2B_Product_Group1=leads_to_insert[i].get("ZProductGrp_KUT", None) or None,
                                C_SFDC___Dealer_Source1=leads_to_insert[i].get("ZDealerSource_KUT", None) or None,
                                C_SAP_Lead_Campaign_Value1=leads_to_insert[i].get("ZCampaignvalue_KUT", None) or None,
                                C_SFDC___Roofing_lead_category1=leads_to_insert[i].get("ZRoofLeadCat_KUT", None) or None,
                                C_SAP_Related_Roofing_Installation1=leads_to_insert[i].get("ZRelRoofInstallation_KUT", None) or None,
                                C_SAP_Related_Roofing_Profile1=leads_to_insert[i].get("ZRelRoofProfile_KUT", None) or None,
                                C_SAP_Related_Roofing_RWS1=leads_to_insert[i].get("ZRelRoofRWS_KUT", None) or None,
                                C_SAP_Related_Roofing_Safety1=leads_to_insert[i].get("ZRelRoofSafety_KUT", None) or None,
                                C_SAP_Related_Roofing_Accessories1=leads_to_insert[i].get("ZRelRoofAccessories_KUT", None) or None,
                                C_SAP_Related_Roofing_Solar1=leads_to_insert[i].get("ZRelRoofSolar_KUT", None) or None,
                                C_SAP_Attachment_I1=leads_to_insert[i].get("ZAttachment1URL_KUT", None) or None,
                                C_SAP_Attachment_II1=leads_to_insert[i].get("ZAttachment2URL_KUT", None) or None,
                                C_SAP_Attachment_III1=leads_to_insert[i].get("ZAttachment3URL_KUT", None) or None,
                                C_SAP_Attachment_IV1=leads_to_insert[i].get("ZAttachment4URL_KUT", None) or None,
                                C_SAP_Attachment_V1=leads_to_insert[i].get("ZAttachment5URL_KUT", None) or None,
                                C_SFDC_iPDF_01_Project_Status1=leads_to_insert[i].get("ZProjectStatus_KUT", None) or None,
                                C_SFDC_iPDF_13_Billing_Address_Name1=leads_to_insert[i].get("ZBillAddrName_KUT", None) or None,
                                C_SFDC_iPDF_09_Billing_Address_Street1=leads_to_insert[i].get("ZBillAddrStreet_KUT", None) or None,
                                C_SFDC_iPDF_11_Billing_Address_City1=leads_to_insert[i].get("ZBillAddrCity_KUT", None) or None,
                                C_SFDC_iPDF_10_Billing_Address_Zip1=leads_to_insert[i].get("ZBillAddrPostcode_KUT", None) or None,
                                C_SFDC_iPDF_12_Billing_Address_Country1=leads_to_insert[i].get("ZBillAddrCtry_KUT", None) or None,
                                C_SFDC_iPDF_03_Installers_at_Site_Earliest_We=leads_to_insert[i].get("ZInstatsiteearlatweek_KUT", None) or None,
                                C_SFDC_iPDF_04_Installers_at_Site_Latest_Week=leads_to_insert[i].get("ZInstatsitelateatweek_KUT", None) or None,
                                C_SFDC_iPDF_02_Products_at_Site_Week1=leads_to_insert[i].get("ZProductsatsiteweek_KUT", None) or None,
                                C_SFDC_iPDF_14_Homing_Date1=leads_to_insert[i].get("ZEloquaHomLettDate_KUT", None) or None,
                                C_SFDC_iPDF_05_Project_Manager_1_Name1=leads_to_insert[i].get("ZProjMan1Name_KUT", None) or None,
                                C_SFDC_iPDF_05_Project_Manager_1_Email1=leads_to_insert[i].get("ZProjMan1Email_KUT", None) or None,
                                C_SFDC_iPDF_06_Project_Manager_1_Mobile1=leads_to_insert[i].get("ZProjMan1Mobile_KUT", None) or None,
                                C_SFDC_iPDF_07_Project_Manager_2_Name_1=leads_to_insert[i].get("ZProjMan2Name_KUT", None) or None,
                                C_SFDC_iPDF_05_Project_Manager_2_Email1=leads_to_insert[i].get("ZProjMan2Email_KUT", None) or None,
                                C_SFDC_iPDF_08_Project_Manager_2_Mobile1=leads_to_insert[i].get("ZProjMan2Mobile_KUT", None) or None,
                                C_SAP_Created_to_Kata_Date1=leads_to_insert[i].get("ZCreatedToKataDate_KUT", None) or None,
                                C_SFDC___Created_To_Kata_Time1=leads_to_insert[i].get("ZCreatedToKataTime_KUT", None) or None,
                                C_Meeting_Date_for_email1=leads_to_insert[i].get("ZAgreedappdate_KUT", None) or None,
                                C_Meeting_time_for_email1=leads_to_insert[i].get("ZAgreedapptime_KUT", None) or None,
                                C_SAP_UTM_Medium_Original1=leads_to_insert[i].get("ZUTMMediumOriginal_KUT", None) or None,
                                C_SAP_UTM_Source_Original1=leads_to_insert[i].get("ZUTMSourceOriginal_KUT", None) or None,
                                C_SAP_UTM_Medium_Recent1=leads_to_insert[i].get("ZUTMMediumRecent_KUT", None) or None,
                                C_SAP_UTM_Source_Recent1=leads_to_insert[i].get("ZUTMSourceRecent_KUT", None) or None,
                                C_EML_GROUP_Professional1=leads_to_insert[i].get("B2B_MP_Consent", None) or None,
                                C_EML_GROUP_Roofs_and_renovations1=leads_to_insert[i].get("B2C_MP_Consent", None) or None,
                                C_SFDC_Survey_Status_Installation1=leads_to_insert[i].get("ZCustomerSurveyStatus_KUT", None) or None,
                                ContactUUID=leads_to_insert[i].get("ContactUUID", None) or None,
                                URI=leads_to_insert[i].get("URI", None) or None,
                                C4C_Task_Name=leads_to_insert[i].get("C4C_Task_Name", None) or None,
                                C4C_Status=leads_to_insert[i].get("C4C_Status", None) or None,
                                TimeInsertedUTC=datetime.utcnow(),
                        )
                        for i in range(0, len(leads_to_insert))
                    ]
                )
                logger.debug("Finish Outbound Bulk Insert {0} records table Lead".format(len(leads_to_insert)))
    else:
        logger.debug("No data to insert/update table Lead")


def upsert_target_gr(session_, data, contact_list_ids):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="tg", data=data)
        target_grs_to_update = upsert_data.get("entries_to_update", [])
        target_grs_to_insert = upsert_data.get("entries_to_insert", [])
        if target_grs_to_update:
            for i in range(0, len(target_grs_to_update)):
                session_.execute(
                    TargetGroup.__table__.update()
                        .values
                            (
                                TargetGroupID=target_grs_to_update[i].get("ID", None) or None,
                                Name=target_grs_to_update[i].get("Description", None) or None,
                                ContactListIDInEloqua=contact_list_ids.get(target_grs_to_update[i].get("Description", None), None) or None
                                    if contact_list_ids
                                    else None,
                                TimeInsertedUTC=datetime.utcnow()
                            )
                        .where(TargetGroup.TargetGroupID == target_grs_to_update[i]["ID"])
                )
            logger.debug("Finish Inbound Bulk Update {0} records to table Target Group".format(len(target_grs_to_update)))
        if target_grs_to_insert:
            session_.execute(
                TargetGroup.__table__.insert(),
                [
                    dict
                        (
                            TargetGroupID=target_grs_to_insert[i].get("ID", None) or None,
                            Name=target_grs_to_insert[i].get("Description", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(target_grs_to_insert))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Target Group".format(len(target_grs_to_insert)))
    else:
        logger.debug("No data to insert/update table Target Group")


def upsert_target_gr_m(session_, data):
    if data:
        # pre-process data
        for i in data:
            i.pop("__metadata")
        non_empty_id_data = [entry for entry in data if entry.get("ContactID", None)]
        clean_data = list(map(dict, set(tuple(entry.items()) for entry in non_empty_id_data)))

        # always truncate table before insert because target group member does not have date filter
        if truncate_target_gr_m(session_=session_):
            session_.execute(
                TargetGroupMember.__table__.insert(),
                [
                    dict
                        (
                            C_SFDCContactID=clean_data[i]["ContactID"],
                            TargetGroupID=clean_data[i].get("TargetGroupID", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(clean_data))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Target Group Member".format(len(clean_data)))
        else:
            logger.debug("Error Inbound Bulk Insert {0} records to table Target Group Member".format(len(clean_data)))
    else:
        logger.debug("No data to insert/update table Target Group Member")


def upsert_mark_p(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="mp", data=data)
        mark_ps_to_update = upsert_data.get("entries_to_update", [])
        mark_ps_to_insert = upsert_data.get("entries_to_insert", [])
        unique_mark_ps_to_update = list(map(dict, set(tuple(entry.items()) for entry in mark_ps_to_update)))
        unique_mark_ps_to_insert = list(map(dict, set(tuple(entry.items()) for entry in mark_ps_to_insert)))

        if unique_mark_ps_to_update:
            for i in range(0, len(unique_mark_ps_to_update)):
                session_.execute(
                    MarketingPermission.__table__.update()
                        .values
                            (
                                GeneralConsent=unique_mark_ps_to_update[i].get("GeneralConsent", None) or None,
                                TimeInsertedUTC=datetime.utcnow()
                            )
                        .where(MarketingPermission.ContactUUID == unique_mark_ps_to_update[i]["BusinessPartnerUUID"])
                )
            logger.debug("Finish Inbound Bulk Update {0} records to table Marketing Permission".format(len(unique_mark_ps_to_update)))
        if unique_mark_ps_to_insert:
            session_.execute(
                MarketingPermission.__table__.insert(),
                [
                    dict
                        (
                            ContactUUID=unique_mark_ps_to_insert[i].get("BusinessPartnerUUID", None) or None,
                            GeneralConsent=unique_mark_ps_to_insert[i].get("GeneralConsent", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(unique_mark_ps_to_insert))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Marketing Permission".format(len(unique_mark_ps_to_insert)))
    else:
        logger.debug("No data to insert/update table Marketing Permission")


def upsert_emp(session_, data, type):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="emp", data=data)
        emp_to_update = upsert_data.get("entries_to_update", [])
        emp_to_insert = upsert_data.get("entries_to_insert", [])
        if emp_to_update:
            for i in range(0, len(emp_to_update)):
                session_.execute(
                    Employee.__table__.update()
                        .values
                            (
                                Name=str(emp_to_update[i]["FirstName"]) + " " + str(emp_to_update[i]["LastName"])
                                    if emp_to_update[i].get("FirstName", None) and emp_to_update[i].get("LastName", None)
                                    # business partner
                                    else emp_to_update[i].get("Name", None) or None,
                                Email=emp_to_update[i].get("Email", None) or None,
                                Department=emp_to_update[i].get("Department", None) or None,
                                Country=emp_to_update[i].get("CountryCode", None) or None,
                                BusinessPartnerID=emp_to_update[i].get("BusinessPartnerID", None) or None,
                                TimeInsertedUTC=datetime.utcnow()
                            )
                        .where(Employee.EmployeeID == emp_to_update[i]["UUID"])
                )
            logger.debug("Finish Inbound Bulk Update {0} records to table Employee type {1}".format(len(emp_to_update), type))
        if emp_to_insert:
            session_.execute(
                Employee.__table__.insert(),
                [
                    dict
                        (
                            EmployeeID=emp_to_insert[i].get("UUID", None) or None,
                            Name=str(emp_to_insert[i]["FirstName"]) + " " + str(emp_to_insert[i]["LastName"])
                                if emp_to_insert[i].get("FirstName", None) and emp_to_insert[i].get("LastName", None)
                                # business partner
                                else emp_to_insert[i].get("Name", None) or None,
                            Email=emp_to_insert[i].get("Email", None) or None,
                            Department=emp_to_insert[i].get("Department", None) or None,
                            Country=emp_to_insert[i].get("CountryCode", None) or None,
                            BusinessPartnerID=emp_to_insert[i].get("BusinessPartnerID", None) or None,
                            TimeInsertedUTC=datetime.utcnow()
                        )
                    for i in range(0, len(emp_to_insert))
                ]
            )
            logger.debug("Finish Inbound Bulk Insert {0} records to table Employee type {1}".format(len(emp_to_insert), type))
    else:
        logger.debug("No data to insert/update Employee type {0}".format(type))


def truncate_target_gr_m(session_):
    try:
        session_.execute(delete(TargetGroupMember))
        return True
    except Exception as e:
        return False