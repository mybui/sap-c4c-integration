import json

from dea.bulk.api import BulkClient
from dea.rest.api.cdo import RestCdoClient
from requests import Session
from requests.auth import HTTPBasicAuth

from logging_config import setup_logging

logger = setup_logging(__name__)


class ElqClient:
    def __init__(self, username, password, base_url):
        self.username = username
        self.password = password
        self.base_url = base_url
        self.bulk_client = BulkClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)
        self.rest_client = RestCdoClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)
        self.eloqua_session = Session()

    def import_contact_list(self, data):
        '''
        Import Contact List to Eloqua
        :param data: data to import
        :return: None
        '''
        duplicate_names = dict()
        if data:
            self.eloqua_session.auth = (self.username, self.password)
            for entry in data:
                response = self.eloqua_session.post(url=self.base_url + "/api/REST/1.0/assets/contact/list",
                                                    headers={"Content-Type": "application/json"},
                                                    data=json.dumps(entry, ensure_ascii=False).encode())
                if response.status_code != 201:
                    if response.status_code == 409:
                        duplicate_message_content = response.content.decode().split(",")
                        for element in duplicate_message_content:
                            if "value" in element:
                                contact_list_existing_name_in_eloqua = element.split(":")[1].split("}")[0].split("\"")[1]
                                logger.debug("Target Group name {0} exists in Eloqua as a Contact List name {1}".format(entry["name"],
                                                                                                                        contact_list_existing_name_in_eloqua))
                                duplicate_names[entry["name"]] = contact_list_existing_name_in_eloqua
            logger.debug("Import Contact List complete with {0} already existed in Eloqua".format(len(duplicate_names.keys())))
        else:
            logger.debug("No Contact List to import")
        return duplicate_names

    def delete_contact_list(self, data):
        '''
        Delete Contact List from Eloqua
        :param data: data to import
        :return: None
        '''
        for entry in data:
            try:
                self.rest_client.delete(url=self.rest_client.rest_url_for(path="assets/contact/list/{0}".format(entry)))
            except Exception as e:
                logger.debug("Error while deleting Contact List: {0}".format(e))
                continue
        logger.debug("Delete Contact List complete")

    def export_contact_list_ids(self, data, existing_id, duplicate_names=None):
        '''
        Export Contact List id from Eloqua
        :param data: name of Contact List
        :return: Option[dict[str: str]] name and matching id of Contact List
        '''
        # pre-process data
        if data:
            if existing_id:
                trans_data = [entry[0] for entry in data]
                result = dict()
                for entry in trans_data:
                    try:
                        result[entry] = self.rest_client.get(url=self.rest_client.rest_url_for(
                            path="assets/contact/lists?search=name=\"{0}\"".format(entry)))["elements"][0]["id"]
                    except:
                        continue
                return result
            else:
                trans_data = [entry[1] for entry in data]
                result = dict()
                for entry in trans_data:
                    if duplicate_names:
                        contact_list_existing_name_in_eloqua = duplicate_names.get(entry, None)
                        # if the Target Group name in C4C already existed in Eloqua under a different name
                        # use Eloqua name to export its id
                        if contact_list_existing_name_in_eloqua:
                            # escape + in url
                            if "+" in contact_list_existing_name_in_eloqua:
                                contact_list_existing_name_in_eloqua = contact_list_existing_name_in_eloqua.replace("+", "%2B")
                            try:
                                result[entry] = self.rest_client.get(url=self.rest_client.rest_url_for(
                                    path="assets/contact/lists?search=name=\"{0}\"".format(contact_list_existing_name_in_eloqua)))["elements"][0]["id"]
                            except Exception as e:
                                logger.debug("Error while exporting Contact List with name in C4C as {0}, "
                                             "and duplicated name in Eloqua as {1}: {2}".format(entry,
                                                                                                contact_list_existing_name_in_eloqua,
                                                                                                e))
                                continue
                        # else use Target Group name in C4C
                        else:
                            try:
                                result[entry] = self.rest_client.get(url=self.rest_client.rest_url_for(
                                    path="assets/contact/lists?search=name=\"{0}\"".format(entry)))["elements"][0]["id"]
                            except Exception as e:
                                logger.debug("Error while exporting Contact List with name in C4C as {0}: {1}".format(entry,
                                                                                                                      e))
                                continue
                    else:
                        try:
                            result[entry] = self.rest_client.get(url=self.rest_client.rest_url_for(
                                path="assets/contact/lists?search=name=\"{0}\"".format(entry)))["elements"][0]["id"]
                        except Exception as e:
                            logger.debug(
                                "Error while exporting Contact List with name in C4C as {0}: {1}".format(entry, e))
                            continue
                return result
        return None

    def get_option_list_id(self, name):
        try:
            option_list_id = self.rest_client.get(url=self.rest_client.rest_url_for(
                path="assets/optionLists?search=name=\"{0}\"".format(name))).get("elements")[0].get("id")
            return option_list_id
        except Exception as e:
            print(e)
            return None

    def import_option_list(self, data, name):
        '''
        Import Option List to Eloqua
        :param data: data to import
        :return: None
        '''
        try:
            # option list name "Ruukki Employees from C4C REST API" already existed
            # only need to updated values in list
            option_list_id = self.get_option_list_id(name=name)
            if option_list_id:
                # update an option list if existed
                data["id"] = option_list_id
                self.rest_client.put(
                    url=self.rest_client.rest_url_for(path="assets/optionList/{0}".format(option_list_id)), json=data)
                logger.debug("Import Option List complete")
        except Exception as e:
            logger.debug("Error while importing Option List: {0}".format(str(e)))

    def delete_option_list(self):
        '''
        Delete Option List to Eloqua
        :return: None
        '''
        try:
            option_list_id = self.get_option_list_id(name="Ruukki Employees from C4C REST API")
            if option_list_id:
                self.rest_client.delete(url=self.rest_client.rest_url_for(
                    path="assets/optionList/{0}".format(option_list_id)))
            logger.debug("Delete Option List complete")
        except Exception as e:
            logger.debug("Error while deleting Option List: {0}".format(str(e)))
            pass

