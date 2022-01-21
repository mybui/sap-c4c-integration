import json
import re
import xml.etree.ElementTree as ET
from contextlib import contextmanager

from requests import Session
from requests.structures import CaseInsensitiveDict

from c4c import c4c_config
from logging_config import setup_logging

logger = setup_logging(__name__)


class C4CClient:
    def __init__(self, username, password, base_url):
        self.username = username
        self.password = password
        self.base_url = base_url
        # C4C Entities URLs
        self.account_uri = base_url + c4c_config.account_c4c_uri
        self.contact_uri = base_url + c4c_config.contact_c4c_uri
        self.lead_uri = base_url + c4c_config.lead_c4c_uri
        self.m_permission_uri = base_url + c4c_config.m_permission_c4c_uri
        self.target_gr_uri = base_url + c4c_config.target_gr_c4c_uri
        self.target_gr_m_uri = base_url + c4c_config.target_gr_m_c4c_uri
        self.mark_p_uri = base_url + c4c_config.mark_p_c4c_uri
        self.emp_uri = base_url + c4c_config.emp_c4c_uri
        self.bus_p_uri = base_url + c4c_config.bus_p_uri

    # manage requests session
    @contextmanager
    def start_requests_session(self):
        session = Session()
        session.auth = (self.username, self.password)
        try:
            yield session
        except Exception:
            raise
        finally:
            session.close()

    def get_csrf_token(self, session_):
        headers = CaseInsensitiveDict({"x-csrf-token": "fetch"})
        response_headers = session_.get(url=self.base_url, headers=headers).headers
        if "x-csrf-token" in response_headers:
            return response_headers["x-csrf-token"]
        else:
            return None

    def get_data_count(self, session_, type):
        if type == "account":
            return session_.get(url=self.account_uri + "/$count").json()
        if type == "contact":
            return session_.get(url=self.contact_uri + "/$count").json()
        if type == "lead":
            return session_.get(url=self.lead_uri + "/$count").json()
        if type == "mp":
            return session_.get(url=self.m_permission_uri + "/$count").json()
        if type == "tg":
            return session_.get(url=self.target_gr_uri + "/$count").json()
        if type == "tgm":
            return session_.get(url=self.target_gr_m_uri + "/$count").json()
        if type == "mp":
            return session_.get(url=self.mark_p_uri + "/$count").json()
        if type == "emp":
            return session_.get(url=self.emp_uri + "/$count").json()
        if type == "bp":
            return session_.get(url=self.bus_p_uri + "/$count").json()
        return None

    def validate_data(self, type, data):
        if type == "contact":
            return [entry for entry in data if entry.get("Email", None)]
        if type == "lead":
            return [entry for entry in data if entry.get("ZConsumerEMail_KUT", None) or entry.get("ContactEMail", None)]
        if type == "tg":
            return [entry for entry in data if entry.get("Description", None)]
        if type == "emp":
            duplicate_name = ["00163EAD-C76A-1EDB-9793-6F537D2E7316", "00163E0F-FF67-1EE6-8AB6-660F54FD6562",
                              "00163E34-9FE3-1ED7-B7FE-B6EBE5DA4C9B"]
            # duplicate UUID for Timo Virtanen, Egle Tooming, Claes Axelsson
            return [entry for entry in data if entry.get("UUID", None) not in duplicate_name]
        return None

    def format_c4c_date_time(self, date_time_string):
        if date_time_string:
            if "T" in date_time_string:
                return "{0}T{1}".format(date_time_string.split("T")[0], date_time_string.split("T")[1].split(".")[0])
            if " " in date_time_string:
                return "{0}T{1}".format(date_time_string.split(" ")[0], date_time_string.split(" ")[1].split(".")[0])
        return None

    def format_c4c_time(self, date_time_string):
        if date_time_string:
            if "PT" not in date_time_string and "H" not in date_time_string and "M" not in date_time_string:
                try:
                    time_elements = date_time_string.split(" ")[1].split(".")[0].split(":")
                    return "PT{0}H{1}M{2}S".format(time_elements[0], time_elements[1], time_elements[2])
                except Exception as e:
                    logger.debug("Error '{0}' while formatting lead with time '{1}'".format(e, date_time_string))
                    logger.debug("Reset it to 'PT00H00M00S'")
                    date_time_string = "PT00H00M00S"
            return date_time_string
        return None

    def get_data(self, session_, type, count=None, date_from=None, leads_to_get_mps=None, leads_to_get_bps=None, existing_emps=None):
        # common params
        params = {"$format": "json",
                  "$top": count,
                  # filter for data changed after given date
                  "$filter": "EntityLastChangedOn ge datetimeoffset'{0}'".format(date_from)}
        if type == "contact":
            params["$select"] = c4c_config.contact_c4c_fields
            # validate valid contacts with emails
            return self.validate_data(type=type,
                                      data=session_.get(url=self.contact_uri, params=params).json().get("d", None).get("results", None))
        if type == "account":
            params["$select"] = c4c_config.account_c4c_fields
            return session_.get(url=self.account_uri, params=params).json().get("d", None).get("results", None)
        if type == "lead":
            params["$select"] = c4c_config.lead_c4c_fields
            # validate valid leads with emails
            return self.validate_data(type=type,
                                      data=session_.get(url=self.lead_uri, params=params).json().get("d", None).get("results", None))
        if type == "tg":
            params["$select"] = c4c_config.target_gr_c4c_fields
            # validate valid target groups with names
            return self.validate_data(type=type,
                                      data=session_.get(url=self.target_gr_uri, params=params).json().get("d", None).get("results", None))
        if type == "tgm":
            tgm_params = {
                "$format": "json",
                "$top": count,
                "$select": c4c_config.target_gr_m_c4c_fields
            }
            return session_.get(url=self.target_gr_m_uri, params=tgm_params).json().get("d", None).get("results", None)
        if type == "mp":
            output = [{"BusinessPartnerUUID": lead["ContactUUID"]} for lead in leads_to_get_mps if lead.get("ContactUUID", None)]
            if output:
                for entry in output:
                    z03_uri = self.check_if_mark_p_existed(session_=session_, contact_uuid=entry["BusinessPartnerUUID"])
                    # if Marketing Permission not existed
                    if not z03_uri and isinstance(z03_uri, bool):
                        entry["BusinessPartner_ID"] = None
                        entry["GeneralConsent"] = None
                    # if Z03 Channel Permisison existed
                    elif z03_uri and isinstance(z03_uri, str):
                        params = {"$format": "json",
                                  "$filter": "BusinessPartnerUUID eq guid'{0}'".format(entry["BusinessPartnerUUID"])}
                        entry["BusinessPartner_ID"] = session_.get(url=self.mark_p_uri, params=params).json().get("d", None).get("results", None)[0].get("BusinessPartner_ID")
                        entry["GeneralConsent"] = session_.get(url=z03_uri, params={"$format": "json"}).json().get("d", None).get("results", None).get("Consent", None)
                    # if Z03 Channel Permisison not existed, but Marketing Permission existed
                    else:
                        params = {"$format": "json",
                                  "$filter": "BusinessPartnerUUID eq guid'{0}'".format(entry["BusinessPartnerUUID"])}
                        entry["BusinessPartner_ID"] = session_.get(url=self.mark_p_uri, params=params).json().get("d", None).get("results", None)[0].get("BusinessPartner_ID")
                        entry["GeneralConsent"] = None
                return output
            return None
        if type == "emp":
            params["$select"] = c4c_config.emp_c4c_fields
            response = session_.get(url=self.emp_uri, params=params).json().get("d", None).get("results", None)
            for entry in response:
                org_params = {"$format": "json",
                              "$select": c4c_config.org_unit_c4c_fields}
                org = session_.get(
                    url=entry.get("EmployeeOrganisationalUnitAssignment", None).get("__deferred", None).get("uri", None),
                    params=org_params).json().get("d", None).get("results", None)
                for chan_type in org:
                    entry["Department"] = chan_type.get("OrgUnitID", None)
                entry.pop("EmployeeOrganisationalUnitAssignment", None)
            return self.validate_data(type="emp", data=response)
        if type == "bp":
            existing_emp_uuids = [entry[1] for entry in existing_emps]
            existing_emp_names = [entry[0] for entry in existing_emps]
            output = [{"UUID": lead["OwnerPartyUUID"]} for lead in leads_to_get_bps if lead.get("OwnerPartyUUID", None) and
                      lead.get("OwnerPartyUUID", None) not in existing_emp_uuids]
            output = list(map(dict, set(tuple(sorted(entry.items())) for entry in output)))
            if output:
                params.pop("$top", None)
                params["$select"] = c4c_config.bus_p_fields
                for entry in output:
                    params["$filter"] = "BusinessPartnerUUID eq guid'{0}'".format(entry["UUID"])
                    response = session_.get(url=self.bus_p_uri, params=params).json().get("d", None).get("results", None)[0]
                    if response.get("ThingType", None) == "COD_PARTNERCONTACT_TT" and \
                            response.get("Name", None) and \
                            response.get("Name", None) not in existing_emp_names:
                        entry["Name"] = response.get("Name", None)
                    else:
                        output.remove(entry)
                return output
            return None

    def pre_process_data_for_post(self, data):
        result = []
        for entry in data:
            # each lead must have group code, name, company
            if entry[2] and entry[3] and entry[6]:
                # do nothing with bad data caused by cache problems
                if entry[1] and entry[61] == "create_leads":
                    logger.debug("Detect lead with TableID {0} caused by cache problem, skip processing".format(entry[0]))
                    continue
                else:
                    # check if country name is valid to proceed
                    if entry[11] and len(entry[11]) == 2:
                        try:
                            result.append(
                                {
                                    "TableID": entry[0],
                                    "ID": entry[1] if entry[1] else None,
                                    "GroupCode": entry[2] if entry[2] else None,
                                    # already filtered for non null values
                                    "Name": entry[3],
                                    "UserStatusCode": entry[4] if entry[4] else None,
                                    "OwnerPartyUUID": str(entry[5]).upper() if entry[5] else None,
                                    # already filtered for non null values
                                    "Company": entry[6],
                                    "AccountPostalAddressElementsStreetName": entry[7] if entry[7] else None,
                                    "AccountCity": entry[8] if entry[8] else None,
                                    "AccountPostalAddressElementsStreetPostalCode": entry[9] if entry[9] else None,
                                    "AccountState": entry[10] if entry[10] else None,
                                    "AccountCountry": entry[11] if entry[11] else None,
                                    # will drop and replace with proper b2b or b2c field
                                    "Email": entry[12] if entry[12] else None,
                                    "ContactFirstName": entry[13] if entry[13] else None,
                                    "ContactLastName": entry[14] if entry[14] else None,
                                    "ContactFunctionalTitleName": entry[15] if entry[15] else None,
                                    "ContactMobile": entry[16] if entry[16] else None,
                                    "ContactPhone": entry[17] if entry[17] else None,
                                    "Note": entry[18] if entry[18] else None,
                                    "ZFieldofWork_KUT": entry[19] if entry[19] else None,
                                    "ZProductGrp_KUT": entry[20] if entry[20] else None,
                                    "ZDealerSource_KUT": entry[21] if entry[21] else None,
                                    "ZCampaignvalue_KUT": entry[22] if entry[22] else None,
                                    "ZRoofLeadCat_KUT": entry[23] if entry[23] else None,
                                    # format string to boolean
                                    "ZRelRoofInstallation_KUT": json.loads(str(entry[24]).lower())
                                    if str(entry[24]).lower() == "false" or str(entry[24]).lower() == "true"
                                    else None,
                                    "ZRelRoofProfile_KUT": json.loads(str(entry[25]).lower())
                                    if str(entry[25]).lower() == "false" or str(entry[25]).lower() == "true"
                                    else None,
                                    "ZRelRoofRWS_KUT": json.loads(str(entry[26]).lower())
                                    if str(entry[26]).lower() == "false" or str(entry[26]).lower() == "true"
                                    else None,
                                    "ZRelRoofSafety_KUT": json.loads(str(entry[27]).lower())
                                    if str(entry[27]).lower() == "false" or str(entry[27]).lower() == "true"
                                    else None,
                                    "ZRelRoofAccessories_KUT": json.loads(str(entry[28]).lower())
                                    if str(entry[28]).lower() == "false" or str(entry[28]).lower() == "true"
                                    else None,
                                    "ZRelRoofSolar_KUT": json.loads(str(entry[29]).lower())
                                    if str(entry[29]).lower() == "false" or str(entry[29]).lower() == "true"
                                    else None,
                                    "ZAttachment1URL_KUT": entry[30] if entry[30] else None,
                                    "ZAttachment2URL_KUT": entry[31] if entry[31] else None,
                                    "ZAttachment3URL_KUT": entry[32] if entry[32] else None,
                                    "ZAttachment4URL_KUT": entry[33] if entry[33] else None,
                                    "ZAttachment5URL_KUT": entry[34] if entry[34] else None,
                                    "ZProjectStatus_KUT": entry[35] if entry[35] else None,
                                    "ZBillAddrName_KUT": entry[36] if entry[36] else None,
                                    "ZBillAddrStreet_KUT": entry[37] if entry[37] else None,
                                    "ZBillAddrCity_KUT": entry[38] if entry[38] else None,
                                    "ZBillAddrPostcode_KUT": entry[39] if entry[39] else None,
                                    "ZBillAddrCtry_KUT": entry[40] if entry[40] else None,
                                    "ZInstatsiteearlatweek_KUT": entry[41] if entry[41] else None,
                                    "ZInstatsitelateatweek_KUT": entry[42] if entry[42] else None,
                                    "ZProductsatsiteweek_KUT": entry[43] if entry[43] else None,
                                    # format date time field to "dateTtimeZ"
                                    "ZEloquaHomLettDate_KUT": self.format_c4c_date_time(date_time_string=entry[44]) if entry[4] else None,
                                    "ZProjMan1Name_KUT": entry[45] if entry[45] else None,
                                    "ZProjMan1Email_KUT": entry[46] if entry[46] else None,
                                    "ZProjMan1Mobile_KUT": entry[47] if entry[47] else None,
                                    "ZProjMan2Name_KUT": entry[48] if entry[48] else None,
                                    "ZProjMan2Email_KUT": entry[49] if entry[49] else None,
                                    "ZProjMan2Mobile_KUT": entry[50] if entry[50] else None,
                                    # format date time fields to "dateTtimeZ"
                                    "ZCreatedToKataDate_KUT": self.format_c4c_date_time(date_time_string=entry[51]) if entry[51] else None,
                                    "ZCreatedToKataTime_KUT": self.format_c4c_time(date_time_string=entry[52]) if entry[52] else None,
                                    "ZAgreedappdate_KUT": self.format_c4c_date_time(date_time_string=entry[53]) if entry[53] else None,
                                    "ZAgreedapptime_KUT": self.format_c4c_time(date_time_string=entry[54]) if entry[54] else None,
                                    "ZUTMMediumOriginal_KUT": entry[55] if entry[55] else None,
                                    "ZUTMSourceOriginal_KUT": entry[56] if entry[56] else None,
                                    "ZUTMMediumRecent_KUT": entry[57] if entry[57] else None,
                                    "ZUTMSourceRecent_KUT": entry[58] if entry[58] else None,
                                    "ContactUUID": entry[59] if entry[59] else None,
                                    "URI": entry[60] if entry[60] else None,
                                    "C4C_Task_Name": entry[61],
                                    "C4C_Status": entry[62] if entry[62] else None,
                                    "B2B_MP_Consent": entry[63] if entry[63] else None,
                                    "B2C_MP_Consent": entry[64] if entry[64] else None,
                                    "ZCustomerSurveyStatus_KUT": entry[65] if entry[65] else None,
                                }
                            )
                        except Exception as e:
                            logger.debug("Error '{0}' while formatting lead with TableID {1}".format(e, entry[0]))
                    else:
                        logger.debug("Error while formatting lead with C_Country '{0}'".format(entry[11]))
        return result

    def post_lead_data(self, session_, token, data):
        patch_count, patch_failed_count, post_count, post_failed_count = 0, 0, 0, 0
        tableid_to_inspect_more = {"existed_mp_for_new_lead": [],
                                   "no_contactuuid_for_new_lead": [],
                                   "no_contactuuid_for_existing_lead": [],
                                   "unfound_z03_uri_for_both_existing_lead_and_mp": [],
                                   "error_creating_mp_for_new_lead": [],
                                   "error_creating_mp_for_existing_lead": [],
                                   "error_updating_z03_for_existing_lead": [],
                                   "error_creating_z03_for_existing_lead": [],
                                   "error_creating_z03_for_new_lead": []}
        headers = CaseInsensitiveDict({"Content-Type": "application/json", "x-csrf-token": token})
        for entry in data:
            # preprocess data
            entry_reformatted = entry.copy()
            entry_reformatted.pop("TableID", None)
            entry_reformatted.pop("ID", None)
            # entry_reformatted.pop("ContactUUID", None)
            entry_reformatted.pop("URI", None)
            entry_reformatted.pop("C4C_Task_Name", None)
            entry_reformatted.pop("C4C_Status", None)
            # reassign to correct b2b or b2c fields
            entry_reformatted.pop("B2B_MP_Consent", None)
            entry_reformatted.pop("B2C_MP_Consent", None)
            entry_reformatted.pop("Email", None)
            # check if uri existed to patch
            entry_table_id = entry.get("TableID") or None
            entry_id = entry.get("ID") or None
            entry_uri = entry.get("URI", None) or None
            entry_task = entry.get("C4C_Task_Name", None) or None
            entry_contact_uuid = entry.get("ContactUUID", None) or None
            entry_email = entry.get("Email", None)
            entry_owner_uuid = entry.get("OwnerPartyUUID", None)
            entry_company = entry.get("Company", None)
            entry_address = entry.get("AccountPostalAddressElementsStreetName", None)

            # catch the exceptional case that a lead that is sent got mixed up with an existing email address
            # thus only Lead ID got emptied, but ContactUUID or URI is not emptied yet
            if not entry_id:
                if entry_uri or entry_contact_uuid:
                    entry_uri = None
                    entry_contact_uuid = None
            # get consent: b2b or b2c
            if "Z103" in entry.get("GroupCode", None) or "Z108" in entry.get("GroupCode", None):
                entry_consent = entry.get("B2B_MP_Consent", None) or None
                entry_reformatted["ContactEMail"] = entry_email
            if "Z101" in entry.get("GroupCode", None):
                entry_consent = entry.get("B2C_MP_Consent", None) or None
                entry_reformatted["ZConsumerEMail_KUT"] = entry_email
            # split company name that is longer than 40 characters
            # into Company and CompanySecondName (is not written to DB)
            if entry_company:
                if len(entry_company) > 40:
                    entry_reformatted["Company"] = entry_company[:40]
                    entry_reformatted["CompanySecondName"] = entry_company[40:]
                    # continue splitting further if CompanySecondName is longer than 40 characters
                    entry_company_second = entry_reformatted["CompanySecondName"]
                    if len(entry_company_second) > 40:
                        entry_reformatted["CompanySecondName"] = entry_company_second[:40]
                        entry_reformatted["CompanyThirdName"] = entry_company_second[40:]
            # split address that is longer than 40 characters
            # into AccountPostalAddressElementsStreetName and AccountPostalAddressElementsStreetSufix (is not written to DB)
            if entry_address:
                if len(entry_address) > 40:
                    entry_reformatted["AccountPostalAddressElementsStreetName"] = entry_address[:40]
                    entry_reformatted["AccountPostalAddressElementsStreetSufix"] = entry_address[40:]
                    # continue splitting further if AccountPostalAddressElementsStreetSufix is longer than 40 characters
                    entry_address_second = entry_reformatted["AccountPostalAddressElementsStreetSufix"]
                    if len(entry_address_second) > 40:
                        entry_reformatted["AccountPostalAddressElementsStreetSufix"] = entry_address_second[:40]
                        entry_reformatted["AccountPostalAddressElementsAdditionalStreetSuffixName"] = entry_address_second[40:]

            # format data as json
            entry_reformatted_json = json.dumps(entry_reformatted, ensure_ascii=False).encode()

            if entry_id and entry_uri and entry_contact_uuid:
                if entry_task == "update_leads":
                    response = session_.patch(url=entry_uri, headers=headers,
                                              data=entry_reformatted_json)
                    if response.status_code != 204:
                        logger.debug("Error with Patch entry with URI to C4C: {0}".format(entry_uri))
                        logger.debug("Error status code {0} with text: {1}".format(response.status_code, response.text))
                        # remove entry from data if failed to Patch to C4C
                        # data.remove(entry)
                        patch_failed_count += 1
                        # continue
                    # change status after patched
                    else:
                        entry["C4C_Status"] = "updated"
                        # set OwnerPartyUUID if not existed
                        if not entry_owner_uuid:
                            params = {"$format": "json"}
                            entry["OwnerPartyUUID"] = session_.get(url=entry_uri, params=params).json().get("d", None).get("results", None).get("OwnerPartyUUID", None)
                        z03_channel_uri = self.check_if_mark_p_existed(session_=session_, contact_uuid=entry_contact_uuid)
                        # patch if Z03 URI existed
                        # (z03_channel_uri is defined as a string)
                        if z03_channel_uri and isinstance(z03_channel_uri, str):
                            z03_patch_response = self.post_z03_p(session_=session_, token=token, c4c_task=entry_task,
                                                                 consent=entry_consent, channel_uri=z03_channel_uri)
                            if not z03_patch_response:
                                tableid_to_inspect_more["error_updating_z03_for_existing_lead"].append(entry_table_id)
                        # create new marketing permission and channel permission if not existed yet
                        # (z03_channel_uri = False from above)
                        elif not z03_channel_uri and z03_channel_uri is not None and isinstance(z03_channel_uri, bool):
                            if entry_contact_uuid:
                                channel_uri, mark_p_object_id = self.post_mark_p(session_=session_, token=token, contact_uuid=entry_contact_uuid)
                                # create/update channel permission after finish creating new marketing permission
                                if channel_uri and mark_p_object_id:
                                    z03_post_response = self.post_z03_p(session_=session_, token=token, c4c_task="create_leads",
                                                                        consent=entry_consent, channel_uri=channel_uri,
                                                                        mark_p_object_id=mark_p_object_id)
                                    if not z03_post_response:
                                        tableid_to_inspect_more["error_creating_z03_for_existing_lead"].append(entry_table_id)
                                else:
                                    tableid_to_inspect_more["error_creating_mp_for_existing_lead"].append(entry_table_id)
                            else:
                                logger.debug("No ContactUUID given cannot proceed with Post or Patch Channel Permission, require more inspection")
                                tableid_to_inspect_more["no_contactuuid_for_existing_lead"].append(entry_table_id)
                        # create new Z03 Channel Permission if not existed yet, but Marketing Permission already existed
                        # (z03_channel_uri = (channel_uri, mark_p_object_id) from above)
                        elif isinstance(z03_channel_uri, tuple):
                            channel_uri, mark_p_object_id = z03_channel_uri
                            z03_post_response = self.post_z03_p(session_=session_, token=token, c4c_task="create_leads",
                                                                consent=entry_consent, channel_uri=channel_uri,
                                                                mark_p_object_id=mark_p_object_id)
                            if not z03_post_response:
                                tableid_to_inspect_more["error_creating_z03_for_existing_lead"].append(entry_table_id)
                        # inspect more if cannot find channel permission
                        else:
                            logger.debug("Unable to get Channel Z03 Permission, require more inspection")
                            tableid_to_inspect_more["unfound_z03_uri_for_both_existing_lead_and_mp"].append(entry_table_id)
                        patch_count += 1
            if not entry_id and not entry_uri and not entry_contact_uuid:
                if entry_task == "create_leads":
                    response = session_.post(url=self.lead_uri, headers=headers,
                                             data=entry_reformatted_json)
                    if response.status_code != 201:
                        logger.debug("Error with Post entry with TableID to C4C: {0}".format(entry.get("TableID", None)))
                        logger.debug("Error status code {0} with text: {1}".format(response.status_code, response.text))
                        # data.remove(entry)
                        post_failed_count += 1
                        # continue
                    else:
                        xml_tree = ET.fromstring(response.content.decode("utf-8"))
                        contact_uuid = xml_tree[-1][0][9].text
                        entry["ID"] = xml_tree[-1][0][1].text
                        entry["URI"] = xml_tree[0].text
                        entry["ContactUUID"] = contact_uuid
                        entry["C4C_Status"] = "created"
                        # set OwnerPartyUUID if not existed
                        if not entry_owner_uuid:
                            params = {"$format": "json"}
                            entry["OwnerPartyUUID"] = session_.get(url=entry["URI"], params=params).json().get("d", None).get("results", None).get("OwnerPartyUUID", None)
                        if contact_uuid:
                            existing_mark_p = self.check_if_mark_p_existed(session_=session_, contact_uuid=contact_uuid)
                            # create new marketing permission and channel permission if not existed yet
                            # (existing_mark_p = False)
                            if not existing_mark_p and existing_mark_p is not None and isinstance(existing_mark_p, bool):
                                channel_uri, mark_p_object_id = self.post_mark_p(session_=session_, token=token, contact_uuid=contact_uuid)
                                # create/update channel permission after finish creating new marketing permission
                                if channel_uri and mark_p_object_id:
                                    z03_post_response = self.post_z03_p(session_=session_, token=token, c4c_task=entry_task,
                                                                        consent=entry_consent, channel_uri=channel_uri,
                                                                        mark_p_object_id=mark_p_object_id)
                                    if not z03_post_response:
                                        tableid_to_inspect_more["error_creating_z03_for_new_lead"].append(entry_table_id)
                                else:
                                    tableid_to_inspect_more["error_creating_mp_for_new_lead"].append(entry_table_id)
                            else:
                                logger.debug("Marketing Permission already existed for a newly create Lead, require more inspection ")
                                tableid_to_inspect_more["existed_mp_for_new_lead"].append(entry_table_id)
                        else:
                            logger.debug("No ContactUUID given cannot proceed with Post or Patch Channel Permission, require more inspection")
                            tableid_to_inspect_more["no_contactuuid_for_new_lead"].append(entry_table_id)
                        post_count += 1
        logger.debug("Finish Patch {0} and Post {1} leads to C4C".format(patch_count, post_count))
        logger.debug("{0} leads failed Patch to C4C".format(patch_failed_count))
        logger.debug("{0} leads failed Post to C4C".format(post_failed_count))
        for problem in tableid_to_inspect_more:
            logger.debug("Detect {0} problem(s) for '{1}': {2}".format(len(tableid_to_inspect_more[problem]),
                                                                       problem,
                                                                       tableid_to_inspect_more[problem]))
        return data

    def delete_lead_data(self, session_, token, data):
        headers = CaseInsensitiveDict({"x-csrf-token": token})
        for entry in data:
            entry_uri = entry.get("URI", None)
            if entry_uri:
                response = session_.delete(url=entry_uri, headers=headers)
                if response.status_code != 204:
                    logger.debug("Error while Delete entry with URI to C4C: {0}".format(entry_uri))
            else:
                logger.debug("No URI to delete")

    def check_if_mark_p_existed(self, session_, contact_uuid):
        '''
        Check if a lead has a Marketing Permission and return it, else return False
        :param session_: request session
        :param contact_uuid: ContactUUID of a lead
        :return: Channel Z03 URI if existed or False if not existed
        '''
        headers = None
        params = {"$format": "json",
                  "$filter": "BusinessPartnerUUID eq guid'{0}'".format(contact_uuid)}
        response = session_.get(url=self.mark_p_uri, headers=headers,
                                params=params).json().get("d", None).get("results", None)
        if response:
            logger.debug("Marketing Permission for ContactUUID {0} existed".format(contact_uuid))
            params = {"$format": "json",
                      "$filter": "Channel eq 'Z03'"}
            channel_uri = response[0].get("ChannelPermission", None).get("__deferred", None).get("uri", None)
            mark_p_object_id = response[0].get("ObjectID", None)
            z03_response = session_.get(url=channel_uri, headers=headers, params=params).json().get("d", None).get("results", None)
            # return z03 uri if existed
            if z03_response:
                z03_uri = z03_response[0].get("__metadata", None).get("uri", None)
                logger.debug("Z03 Channel Permission for ContactUUID {0} existed".format(contact_uuid))
                return z03_uri
            # else return channel uri and marketing permission objectid for z03 permission creation
            else:
                logger.debug("Z03 Channel Permission for ContactUUID {0} not existed".format(contact_uuid))
                return channel_uri, mark_p_object_id
        else:
            logger.debug("Marketing Permission for ContactUUID {0} not existed".format(contact_uuid))
            return False

    def post_mark_p(self, session_, token, contact_uuid):
        headers = CaseInsensitiveDict({"Content-Type": "application/json", "x-csrf-token": token})
        data = json.dumps({"BusinessPartnerUUID": contact_uuid}, ensure_ascii=False).encode()
        response = session_.post(url=self.mark_p_uri, headers=headers, data=data)
        if response.status_code != 201:
            logger.debug("Error with Post new Marketing Permission with ContactUUID to C4C: {0}".format(contact_uuid))
            logger.debug("Error status code {0} with text: {1}".format(response.status_code, response.text))
            return None, None
        else:
            logger.debug("Finish Post 1 Marketing Permission with ContactUUID to C4C: {0}".format(contact_uuid))
            xml_tree = ET.fromstring(response.content.decode("utf-8"))
            channel_uri = xml_tree[0].text + "/ChannelPermission"
            mark_p_object_id = re.findall(r"'(.*?)'", channel_uri)[0]
            return channel_uri, mark_p_object_id

    def post_z03_p(self, session_, token, c4c_task, consent, channel_uri, mark_p_object_id=None):
        if consent == "1":
            encoded_consent = "1"
        elif consent == "0":
            encoded_consent = "2"
        else:
            encoded_consent = "3"
        headers = CaseInsensitiveDict({"Content-Type": "application/json",
                                       "x-csrf-token": token})
        data = json.dumps({"ParentObjectID": mark_p_object_id, "Channel": "Z03", "Consent": encoded_consent}, ensure_ascii=False).encode()
        if c4c_task == "create_leads":
            response = session_.post(url=channel_uri, headers=headers, data=data)
            if response.status_code != 201:
                logger.debug("Error with Post 1 Channel Z03 Permission with channel URI to C4C: {0}".format(channel_uri))
                logger.debug("Error status code {0} with text: {1}".format(response.status_code, response.text))
                return False
            else:
                logger.debug("Finish Post 1 Channel Z03 Permission with URI to C4C: {0}".format(channel_uri))
                return True
        if c4c_task == "update_leads":
            data = json.dumps({"Channel": "Z03", "Consent": encoded_consent}, ensure_ascii=False).encode()
            response = session_.patch(url=channel_uri, headers=headers, data=data)
            if response.status_code != 204:
                logger.debug("Error with Patch 1 Channel Z03 Permission with URI in C4C: {0}".format(channel_uri))
                logger.debug("Error status code {0} with text: {1}".format(response.status_code, response.text))
                return False
            else:
                logger.debug("Finish Patch 1 Channel Z03 Permission with URI to C4C: {0}".format(channel_uri))
                return True
