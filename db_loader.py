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
        # date_from = "2020-01-01T00:00:00Z"
        date_from_tg = None
        date_to_tg = None
    else:
        # current date time for daily upsert to DB
        # last one day
        past_time = datetime.utcnow() - timedelta(hours=24)
        # C4C date time format
        date_from = str(past_time.date()) + "T" + str(past_time.time()).split(".")[0] + "Z"
        # Eloqua data time format
        date_from_tg = str(past_time.date()) + " " + str(past_time.time()).split(".")[0]
        date_to_tg = str(current_time.date()) + " " + str(current_time.time()).split(".")[0]

    retriever = C4CClient(username=C4C_USER, password=C4C_PASSWORD, base_url=C4C_BASE_URL)
    elq_client = ElqClient(username=ELQ_USER, password=ELQ_PASSWORD, base_url=ELQ_BASE_URL)

    with retriever.start_requests_session() as requests_session:
        if first_run:
            # get contacts and target groups from 2020 for first run
            contacts = retriever.get_data(session_=requests_session, type="contact",
                                          # max data to return
                                          count=retriever.get_data_count(session_=requests_session, type="contact"),
                                          date_from="2020-01-01T00:00:00Z")
            accounts = retriever.get_data(session_=requests_session, type="account",
                                          count=retriever.get_data_count(session_=requests_session, type="account"),
                                          date_from=date_from)
            target_gr_ms = retriever.get_data(session_=requests_session, type="tgm",
                                              count=retriever.get_data_count(session_=requests_session, type="tgm"))
            target_grs = retriever.get_data(session_=requests_session, type="tg",
                                            count=retriever.get_data_count(session_=requests_session, type="tg"),
                                            date_from="2020-01-01T00:00:00Z")
        else:
            contacts = retriever.get_data(session_=requests_session, type="contact",
                                          count=retriever.get_data_count(session_=requests_session, type="contact"),
                                          date_from=date_from)
            accounts = retriever.get_data(session_=requests_session, type="account",
                                          count=retriever.get_data_count(session_=requests_session, type="account"),
                                          date_from=date_from)
            target_gr_ms = retriever.get_data(session_=requests_session, type="tgm",
                                              count=retriever.get_data_count(session_=requests_session, type="tgm"))
            target_grs = retriever.get_data(session_=requests_session, type="tg",
                                            count=retriever.get_data_count(session_=requests_session, type="tg"),
                                            date_from=date_from)
        with start_psql_session() as session:
            upsert_contact(session_=session, data=contacts)
            upsert_account(session_=session, data=accounts)
            upsert_target_gr_m(session_=session, data=target_gr_ms)

            # upsert target groups with existing IDs in Eloqua
            target_grs_existing_in_db = get_data_by_time(session_=session, purpose="eloqua_export",
                                                         type="tg", date_from=date_from_tg,
                                                         date_to=date_to_tg)
            contact_list_ids = elq_client.export_contact_list_ids(data=target_grs_existing_in_db, existing_id=True)
            upsert_target_gr(session_=session, data=target_grs, contact_list_ids=contact_list_ids)
            # upsert target groups without existing IDs in Eloqua
            target_grs_without_ids_in_eloqua = get_data_for_eloqua_import(session_=session, type="tg")
            duplicate_names = elq_client.import_contact_list(data=prepare_data_elq_import(type="tg", data=target_grs_without_ids_in_eloqua))
            new_contact_list_ids = elq_client.export_contact_list_ids(data=target_grs_without_ids_in_eloqua,
                                                                      existing_id=False,
                                                                      duplicate_names=duplicate_names)
            upsert_target_gr(session_=session,
                             data=[{"ID": i[0], "Description": i[1]} for i in target_grs_without_ids_in_eloqua],
                             contact_list_ids=new_contact_list_ids)