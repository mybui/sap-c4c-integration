import argparse
from datetime import datetime, timedelta

from c4c.c4c_client import C4CClient
from elq.elq_client import ElqClient
from db.db_crud import *
from settings import C4C_USER, C4C_PASSWORD, C4C_BASE_URL, ELQ_USER, ELQ_PASSWORD, ELQ_BASE_URL


def prepare_data_elq_import(type, data):
    if type == "tg" and data:
        return [{"name": i[1], "description": "Imported from C4C REST API", "folderId": "3693"} for i in data]
    if type == "emp" and data:
        # define put body to add employees to an option list
        result = {"name": "Ruukki Employees from C4C REST API", "elements":
            [{"type": "Option", "displayName": "-- Please select --", "value": "No selection"}]}
        for entry in data:
            if entry[0] and entry[1]:
                result["elements"].append({"type": "Option", "displayName": str(entry[0]), "value": str(entry[1])})
            else:
                continue
        return result
    return None


if __name__ == "__main__":
    # add first run arg
    parser = argparse.ArgumentParser()
    parser.add_argument("--first_run", type=int, help="input 1 for first run, default is 0", nargs='?', default=0)
    first_run = parser.parse_args().first_run
    current_time = datetime.utcnow()
    if first_run:
        # start date time when data first written to C4C: last one month e.g. 2020-07-06T00:00:00Z
        first_run_date_from = current_time - timedelta(days=30)
        date_from = str(first_run_date_from.date()) + "T" + str(first_run_date_from.time()).split(".")[0] + "Z"
        date_from_tg = None
        date_to_tg = None
    else:
        # current date time for daily upsert to DB
        # last 35 minutes
        past_time = datetime.utcnow() - timedelta(minutes=35)
        # C4C date time format
        date_from = str(past_time.date()) + "T" + str(past_time.time()).split(".")[0] + "Z"
        # Eloqua data time format
        date_from_tg = str(past_time.date()) + " " + str(past_time.time()).split(".")[0]
        date_to_tg = str(current_time.date()) + " " + str(current_time.time()).split(".")[0]

    retriever = C4CClient(username=C4C_USER, password=C4C_PASSWORD, base_url=C4C_BASE_URL)
    elq_client = ElqClient(username=ELQ_USER, password=ELQ_PASSWORD, base_url=ELQ_BASE_URL)

    with retriever.start_requests_session() as requests_session:
        if first_run:
            leads = retriever.get_data(session_=requests_session, type="lead",
                                       count=retriever.get_data_count(session_=requests_session, type="lead"),
                                       date_from=date_from)
            mark_ps = retriever.get_data(session_=requests_session, type="mp",
                                         count=retriever.get_data_count(session_=requests_session, type="mp"),
                                         leads_to_get_mps=leads)
            emps = retriever.get_data(session_=requests_session, type="emp",
                                      count=retriever.get_data_count(session_=requests_session, type="emp"),
                                      date_from="2020-01-01T00:00:00Z")
        else:
            leads = retriever.get_data(session_=requests_session, type="lead",
                                       count=retriever.get_data_count(session_=requests_session, type="lead"),
                                       date_from=date_from)
            mark_ps = retriever.get_data(session_=requests_session, type="mp",
                                         count=retriever.get_data_count(session_=requests_session, type="mp"),
                                         leads_to_get_mps=leads)
            emps = retriever.get_data(session_=requests_session, type="emp",
                                      count=retriever.get_data_count(session_=requests_session, type="emp"),
                                      date_from=date_from)
        with start_psql_session() as session:
            upsert_lead(session_=session, data=leads, inbound=True)
            upsert_mark_p(session_=session, data=mark_ps)

            # upsert employees
            upsert_emp(session_=session, data=emps, type="Employee")
            # upsert business partners
            bps = retriever.get_data(session_=requests_session, type="bp", leads_to_get_bps=leads,
                                     existing_emps=get_data_for_eloqua_import(session_=session, type="emp"))
            upsert_emp(session_=session, data=bps, type="Business Partner")
            # add all employees and business partners to an option list in Eloqua
            all_emps_and_bps = get_data_for_eloqua_import(session_=session, type="emp")
            elq_client.import_option_list(data=prepare_data_elq_import(type="emp", data=all_emps_and_bps),
                                          name="Ruukki Employees from C4C REST API")