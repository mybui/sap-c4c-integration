# contact
contact_c4c_uri = "ContactCollection"
# no space allowed
contact_c4c_fields = "ContactID,AccountUUID,AccountID,BusinessAddressStreet,BusinessAddressCity," \
                     "BusinessAddressCountryCode,Email,ZFieldofWork,FirstName,LastName,JobTitle,Mobile," \
                     "ContactOwnerUUID,Phone,ZisPlannjaDealer,StatusCode,BusinessAddressStreetPostalCode," \
                     "ZPlannjaPricelist,ContactUUID,EntityLastChangedOn"

# account
account_c4c_uri = "CorporateAccountCollection"
account_c4c_fields = "AccountID,AddressLine1,ZBCABCClassification,City,CountryCode,Name,OwnerUUID,ZRCCustomerSegments_KUT," \
                     "RoleCode,ZRRABCClassification,ZRRTacticalClassification,LifeCycleStatusCode,StreetPostalCode," \
                     "EntityLastChangedOn"

# lead
lead_c4c_uri = "LeadCollection"
lead_c4c_fields = "ID,GroupCode,Name,UserStatusCode,OwnerPartyUUID,Company,AccountPostalAddressElementsStreetName,AccountCity," \
                  "AccountPostalAddressElementsStreetPostalCode,AccountState,AccountCountry,ZConsumerEMail_KUT,ContactEMail," \
                  "ContactFirstName,ContactLastName,ContactFunctionalTitleName,ContactMobile,ContactPhone,Note," \
                  "ZFieldofWork_KUT,ZProductGrp_KUT,ZDealerSource_KUT,ZCampaignvalue_KUT,ZRoofLeadCat_KUT," \
                  "ZRelRoofInstallation_KUT,ZRelRoofProfile_KUT,ZRelRoofRWS_KUT,ZRelRoofSafety_KUT,ZRelRoofAccessories_KUT," \
                  "ZRelRoofSolar_KUT,ZAttachment1URL_KUT,ZAttachment2URL_KUT,ZAttachment3URL_KUT,ZAttachment4URL_KUT," \
                  "ZAttachment5URL_KUT,ZProjectStatus_KUT,ZBillAddrName_KUT,ZBillAddrName_KUT,ZBillAddrStreet_KUT,ZBillAddrStreet_KUT," \
                  "ZBillAddrCity_KUT,ZBillAddrCity_KUT,ZBillAddrPostcode_KUT,ZBillAddrCity_KUT,ZBillAddrCtry_KUT," \
                  "ZInstatsiteearlatweek_KUT,ZInstatsitelateatweek_KUT,ZProductsatsiteweek_KUT,ZEloquaHomLettDate_KUT," \
                  "ZProjMan1Name_KUT,ZProjMan1Email_KUT,ZProjMan1Mobile_KUT,ZProjMan2Name_KUT,ZProjMan2Email_KUT," \
                  "ZProjMan2Mobile_KUT,ZCreatedToKataDate_KUT,ZCreatedToKataTime_KUT,ZAgreedappdate_KUT,ZAgreedapptime_KUT," \
                  "ZUTMMediumOriginal_KUT,ZUTMSourceOriginal_KUT,ZUTMMediumRecent_KUT,ZUTMSourceRecent_KUT," \
                  "ZSendEmailToDealer_KUT,ZCustomerSurveyStatus_KUT,ContactUUID,EntityLastChangedOn"

# marketing permission
m_permission_c4c_uri = "MarketingPermissionCollection"
m_permission_c4c_fields = "EntityLastChangedOn"

# target group
target_gr_c4c_uri = "TargetGroupCollection"
target_gr_c4c_fields = "ID,Description,EntityLastChangedOn"

# target group member
target_gr_m_c4c_uri = "TargetGroupMemberCollection"
target_gr_m_c4c_fields = "ContactID,TargetGroupID"

# marketing permission
mark_p_c4c_uri = "MarketingPermissionCollection"
mark_p_c4c_fields = "BusinessPartnerUUID,BusinessPartner_ID,ChannelPermission,EntityLastChangedOn"

# channel permission
chan_p_c4c_fields = "Channel,Consent"

# employee
emp_c4c_uri = "EmployeeCollection"
emp_c4c_fields = "UUID,FirstName,LastName,Email,CountryCode,BusinessPartnerID," \
                 "EmployeeOrganisationalUnitAssignment,EntityLastChangedOn"

# business partner
bus_p_uri = "BusinessPartnerCollection"
bus_p_fields = "BusinessPartnerUUID,Name,ThingType"

# organization unit
org_unit_c4c_fields = "OrgUnitID"