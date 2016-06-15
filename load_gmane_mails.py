import os

import pandas as pd
import requests


def fetch_emails(projects_data_filename, gmane_base_url, storage_path):
    projects_data = pd.read_csv(projects_data_filename)
    for row_id, project_data in projects_data.iterrows():
        project_messages_url = '{base_url}/{list_id}/{start_id}/{end_id}'.format(
            base_url=gmane_base_url,
            list_id=project_data.list_id,
            start_id=project_data.start_id,
            end_id=project_data.end_id)
        print('fetching {}...'.format(project_messages_url))
        mails = requests.get(project_messages_url)
        project_messages_filename = os.path.join(
            storage_path,
            project_data.list_id+'.mbox')
        with open(project_messages_filename, 'w') as pm:
            pm.write(mails.text)

if __name__ == '__main__':
    storage_path = '/home/zormit/bigdata/innovation-thesis/'
    projects_data_filename = '/home/zormit/ownCloud/Uni/msemester5/innovation-thesis/data/projects.csv'

    gmane_base_url = 'http://download.gmane.org'

    fetch_emails(projects_data_filename, gmane_base_url, storage_path)
