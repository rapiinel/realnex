# %% [markdown]
# #### Initial Imports and Constants

# %%
import pandas as pd
import requests
import concurrent.futures
from tqdm import tqdm
import cloudscraper
import numpy as np
import os
import json
import warnings

warnings.filterwarnings('ignore')

with open('config.json') as config_file:
    config = json.load(config_file)

scraper = cloudscraper.create_scraper()

# %%
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-PH,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    "authorization": config["AUTHORIZATION"],
    'Cookie':config["COOKIE"],
    'X-SelectedDb-Key': config["SELECTED_DB"]
}

# %% [markdown]
# #### Getting Contact Records

# %%
def get_properties():
    """
    Fetch properties from the RealNex CRM API using multithreading for efficiency.

    This function fetches properties from the RealNex CRM API by making multiple concurrent requests.
    It handles pagination and collects all the data into a single list of DataFrames.

    Returns:
        list of pd.DataFrame: A list where each element is a DataFrame containing contact data from a single API response page.

    Nested Functions:
        fetch_page(page):
            Fetch a single page of properties from the API.

            Args:
                page (int): The page offset for the API request.

            Returns:
                pd.DataFrame: A DataFrame containing the properties data from the API response.
    """
    def fetch_page(page):
        """
        Fetch a single page of properties from the RealNex CRM API.

        Args:
            page (int): The page offset for the API request.

        Returns:
            pd.DataFrame: A DataFrame containing the properties data from the API response.
        """        
        url = f'https://sync.realnex.com/api/v1/CrmOData/Properties?api-version=1.0&$skip={page}'
        payload= {}
        response = requests.get(url, headers=headers, data=payload)
        return pd.DataFrame(response.json()['value'])

    temp_list = []
    page = 0
    limit = 1  # Initialize limit to enter the while loop

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while limit != 0:
            future_to_page = {executor.submit(fetch_page, page): page for page in range(page, page + 500, 50)}
            for future in concurrent.futures.as_completed(future_to_page):
                data = future.result()
                limit = len(data)
                if limit > 0:
                    temp_list.append(data)
            page += 500  # Increment by 500 because we are fetching 10 pages in parallel
            
    return pd.concat(temp_list, ignore_index=True)

    temp_list = []
    page = 0
    limit = 1  # Initialize limit to enter the while loop

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while limit != 0:
            future_to_page = {executor.submit(fetch_page, page): page for page in range(page, page + 500, 50)}
            for future in concurrent.futures.as_completed(future_to_page):
                data = future.result()
                limit = len(data)
                if limit > 0:
                    temp_list.append(data)
            page += 500  # Increment by 500 because we are fetching 10 pages in parallel
            
    return pd.concat(temp_list, ignore_index=True)

# %%
def get_contacts():
    """
    Fetch contacts from the RealNex CRM API using multithreading for efficiency.

    This function fetches contacts from the RealNex CRM API by making multiple concurrent requests.
    It handles pagination and collects all the data into a single list of DataFrames.

    Returns:
        list of pd.DataFrame: A list where each element is a DataFrame containing contact data from a single API response page.

    Nested Functions:
        fetch_page(page):
            Fetch a single page of contacts from the API.

            Args:
                page (int): The page offset for the API request.

            Returns:
                pd.DataFrame: A DataFrame containing the contacts data from the API response.
    """
    def fetch_page(page):
        """
        Fetch a single page of contacts from the RealNex CRM API.

        Args:
            page (int): The page offset for the API request.

        Returns:
            pd.DataFrame: A DataFrame containing the contacts data from the API response.
        """        
        url = f'https://sync.realnex.com/api/v1/CrmOData/Contacts?api-version=1.0&$skip={page}'
        payload= {}
        response = requests.get(url, headers=headers, data=payload)
        return pd.DataFrame(response.json()['value'])

    temp_list = []
    page = 0
    limit = 1  # Initialize limit to enter the while loop

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while limit != 0:
            future_to_page = {executor.submit(fetch_page, page): page for page in range(page, page + 500, 50)}
            for future in concurrent.futures.as_completed(future_to_page):
                data = future.result()
                limit = len(data)
                if limit > 0:
                    temp_list.append(data)
            page += 500  # Increment by 500 because we are fetching 10 pages in parallel
            
    return pd.concat(temp_list, ignore_index=True)

# %% [markdown]
# #### Uploading Notes

# %%
def add_notes(contact_key, date, subject, notes):
    """
    Adds notes to a contact's history in the RealNex CRM.

    Parameters:
    contact_key (str): The unique identifier for the contact.
    date (str): The date for the note in 'Y-m-d' format.
    subject (str): The subject of the note.
    notes (str): The detailed notes to be added.

    Returns:
    response (requests.Response): The response from the RealNex API after posting the note.
    """
    data = {
            "eventTypeKey": 18,
            "published":True,
            "timeless":True,
            "startDate": date,
            "endDate": date,
            "subject": subject,
            "notes": notes,
            "statusKey": 0
    }

    url = f'https://sync.realnex.com/api/v1/Crm/object/{contact_key}/history'
    response = scraper.post(url, headers=headers, json=data)

    return response

# %%
def delete_contact(contact_key):
    url = f'https://sync.realnex.com/api/v1/Crm/contact/{contact_key}'
    response = scraper.delete(url, headers=headers)

    return response    

# %% [markdown]
# ##### Testing delete contact function


def add_task(contact_key, date, subject, notes):
    """
    Adds notes to a contact's history in the RealNex CRM.

    Parameters:
    contact_key (str): The unique identifier for the contact.
    date (str): The date for the note in 'Y-m-d' format.
    subject (str): The subject of the note.
    notes (str): The detailed notes to be added.

    Returns:
    response (requests.Response): The response from the RealNex API after posting the note.
    """
    data = {
            "eventTypeKey": 18,
            "published":True,
            "timeless":True,
            "startDate": date,
            "endDate": date,
            "subject": subject,
            "notes": notes,
            "statusKey": 0
    }

    url = f'https://sync.realnex.com/api/v1/Crm/object/{contact_key}/event'
    response = scraper.post(url, headers=headers, json=data)

    return response

def delete_task(contact_key):

    url = f'https://sync.realnex.com/api/v1/Crm/event/{contact_key}'
    response = scraper.delete(url, headers=headers)

    return response

def get_contact_activity(contact_id):
    url = f'https://crm.realnex.com/api/v1/contact/EventsDataTable'

    payload = {
        "draw": 1,
        "columns": [
            {"data": "Key", "name": "Key", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}},
            {"data": "IsAlarm", "name": "IsAlarm", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}},
            {"data": "StartDate", "name": "start_date", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Day", "name": "Day", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}},
            {"data": "EndDate", "name": "end_date", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Age", "name": "Age", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "EventType", "name": "event_types.event_type_name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Subject", "name": "subject", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "LinkedToUrl", "name": "LinkedTo", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}},
            {"data": "ProjectLead", "name": "project_info.project", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Notes", "name": "notes", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}},
            {"data": "Owner", "name": "user_info.user_name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Team", "name": "group_info.name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Priority", "name": "priorities.priority", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "IsParticipant", "name": "IsParticipant", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "IsFinished", "name": "IsFinished", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "Key", "name": "Edit", "searchable": True, "orderable": False, "search": {"value": "", "regex": False}}
        ],
        "order": [{"column": 2, "dir": "asc"}],
        "start": 0,
        "length": 10,
        "search": {"value": "", "regex": False},
        "block_id": "events_block",
        "context": "contact",
        "PageIndex": 1,
        "PageSize": 10000,
        "Key": contact_id,
        "table-page": "contact",
        "table-object-type": "event"
    }

    response = scraper.post(url, headers=headers, json=payload)
    return response

def get_property_enriched_fields(property_key):
    url = 'https://crm.realnex.com/api/v1/property/details'
    params = {
        'context': 'property',
        'block_id': 'property_details',
        'Key': property_key,
        'hasLayout': 'true'
    }

    response = requests.get(url, headers=headers, params=params).json()['Data']

    # print(response)
    dataframe_structure = {
        'Key' : [],
        'State Class' : [],
        'Legal Description' : [],
        'Building Area': [],
        'Land Area': [],
        'Land Value': [],
        'Improvement': [],
        'Market': [],
        'Appraised': [],
        'Year Build': [],
        'Appraisal District Account Id':[]
        }

    dataframe_structure['Key'].append(response.get('Key', np.nan))
    dataframe_structure['State Class'].append(response.get('UserFields', np.nan).get('User3', np.nan))
    dataframe_structure['Legal Description'].append(response.get('UserFields', np.nan).get('User5', np.nan))
    dataframe_structure['Building Area'].append(response.get('UserDataFields', np.nan).get('UserNumber6', np.nan))
    dataframe_structure['Land Area'].append(response.get('UserDataFields', np.nan).get('UserNumber7', np.nan))
    dataframe_structure['Land Value'].append(response.get('UserDataFields', np.nan).get('UserNumber8', np.nan))
    dataframe_structure['Improvement'].append(response.get('UserDataFields', np.nan).get('UserNumber9', np.nan))
    dataframe_structure['Market'].append(response.get('UserDataFields', np.nan).get('UserNumber10', np.nan))
    dataframe_structure['Appraised'].append(response.get('UserDataFields', np.nan).get('UserNumber11', np.nan))
    dataframe_structure['Year Build'].append(response.get('UserDataFields', np.nan).get('UserNumber12', np.nan))
    dataframe_structure['Appraisal District Account Id'].append(response.get('UserFields', np.nan).get('User14', np.nan))

    return pd.DataFrame(dataframe_structure)

#### Getting the properties linked to a contact
def get_linked_properties(contact_key):
    data = {
        'length': '10',
        'Key': contact_key
        }
    url = 'https://crm.realnex.com/api/v1/contact/LinksDataTable'

    response = scraper.post(url, headers=headers, data=data)
    
    return response.json()



def test_function():
    print("11/9/2024 test 2")



# %%
if __name__ == '__main__':
    property_df = get_properties()
    print(property_df.head())