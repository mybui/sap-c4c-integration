from copy import deepcopy
from datetime import datetime, timedelta

from logging_config import setup_logging

from c4c.c4c_client import C4CClient
from db.db_crud import start_psql_session, get_data_for_c4c_import, get_data_to_try_again,\
    upsert_lead, upsert_mark_p, get_data_for_eloqua_import, upsert_emp
from db_loader import prepare_data_elq_import
from elq.elq_client import ElqClient
from settings import C4C_USER, C4C_PASSWORD, C4C_BASE_URL, ELQ_USER, ELQ_PASSWORD, ELQ_BASE_URL

logger = setup_logging(__name__)

if __name__ == "__main__":
    c4c_client = C4CClient(username=C4C_USER, password=C4C_PASSWORD, base_url=C4C_BASE_URL)
    elq_client = ElqClient(username=ELQ_USER, password=ELQ_PASSWORD, base_url=ELQ_BASE_URL)

    with start_psql_session() as session:
        pending_data = get_data_for_c4c_import(session_=session)
        formatted_pending_data = c4c_client.pre_process_data_for_post(data=pending_data)

    if formatted_pending_data:
        with c4c_client.start_requests_session() as requests_session:
            token = c4c_client.get_csrf_token(session_=requests_session)
            if token:
                # change lead status to "processing" so that it is no longer "pending"
                # and not picked up by the next run (5 minute interval can be too short
                # if there is a sudden huge amount of leads "pending" in DB due to Eloqua programs),
                # and commit this separately from other sql session
                with start_psql_session() as session:
                    processing_data_to_db = deepcopy(formatted_pending_data)
                    for entry in processing_data_to_db:
                        entry["C4C_Status"] = "processing"
                    upsert_lead(session_=session, data=processing_data_to_db, outbound=True)

                # import lead
                first_try_output_c4c = c4c_client.post_lead_data(session_=requests_session, token=token,
                                                                 data=formatted_pending_data)

                # sometimes leads are valid but still cannot be sent somehow
                # retry once again here
                second_try_data_c4c = [item for item in first_try_output_c4c if item["C4C_Status"] == "pending"]
                if second_try_data_c4c:
                    second_try_output_c4c = c4c_client.post_lead_data(session_=requests_session, token=token,
                                                                      data=second_try_data_c4c)

                    # remove leads that are failed in first try but are successful in second try: remove and update
                    for table_id in [item["TableID"] for item in second_try_output_c4c if item["C4C_Status"] != "pending"]:
                        for item in first_try_output_c4c:
                            if item["TableID"] == table_id:
                                # remove leads that are failed in first
                                first_try_output_c4c.remove(item)

                    # update those that are successful in second try
                    first_try_output_c4c += [item for item in second_try_output_c4c if item["C4C_Status"] != "pending"]

                # after sending leads (even successful or not)
                # continue to update leads information in DB
                # for the next run to pick up the right leads
                with start_psql_session() as session:
                    upsert_lead(session_=session, data=first_try_output_c4c, outbound=True)
                    # upsert marketing permissions, employees and business partners
                    # resulted from new leads in past 5 minutes
                    past_time = datetime.utcnow() - timedelta(minutes=5)
                    date_from = str(past_time.date()) + "T" + str(past_time.time()) + "Z"
                    # leads
                    leads = c4c_client.get_data(session_=requests_session, type="lead",
                                                count=c4c_client.get_data_count(session_=requests_session, type="lead"),
                                                date_from=date_from)
                    # marketing permissions
                    mark_ps = c4c_client.get_data(session_=requests_session, type="mp",
                                                  count=c4c_client.get_data_count(session_=requests_session, type="mp"),
                                                  leads_to_get_mps=leads)
                    upsert_mark_p(session_=session, data=mark_ps)
                    # employees
                    emps = c4c_client.get_data(session_=requests_session, type="emp",
                                               count=c4c_client.get_data_count(session_=requests_session, type="emp"),
                                               date_from=date_from)
                    upsert_emp(session_=session, data=emps, type="Employee")
                    # business partners
                    bps = c4c_client.get_data(session_=requests_session, type="bp", leads_to_get_bps=leads,
                                              existing_emps=get_data_for_eloqua_import(session_=session, type="emp"))
                    upsert_emp(session_=session, data=bps, type="Business Partner")
                    # update all new employees and business partners to an option list in Eloqua
                    all_emps_and_bps = get_data_for_eloqua_import(session_=session, type="emp")
                    elq_client.import_option_list(data=prepare_data_elq_import(type="emp", data=all_emps_and_bps),
                                                  name="Ruukki Employees from C4C REST API")

                # lastly, check if there are still leads with status as "processing" and unable to be sent
                # they should be marked as "pending" to be processed again later
                # and commit this separately from other sql session
                with start_psql_session() as session:
                    data_to_try_again = get_data_to_try_again(session_=session)
                    if data_to_try_again:
                        formatted_data_to_try_again = c4c_client.pre_process_data_for_post(data=data_to_try_again)
                        upsert_lead(session_=session, data=formatted_data_to_try_again, outbound=True)
            else:
                logger.debug("No C4C Token to import data")
    else:
        logger.debug("No Lead with 'pending' status to import")
