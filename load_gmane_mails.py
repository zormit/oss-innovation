import os

import pandas as pd
import requests
import mailbox


def fetch_mails_range(base_url, list_id, start_id, end_id):
    project_messages_url = '{base_url}/{list_id}/{start_id}/{end_id}'.format(
        base_url=base_url,
        list_id=list_id,
        start_id=start_id,
        end_id=end_id)
    print('fetching {}...'.format(project_messages_url))
    mails = requests.get(project_messages_url)
    project_messages_filename = os.path.join(
        storage_path,
        "{}_{}-{}.batch.mbox".format(list_id, start_id, end_id))
    with open(project_messages_filename, 'w') as pm:
        pm.write(mails.text)


def fetch_mails(projects_data_filename, base_url, storage_path, batchsize=3000):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        if project_data.finished_downloading:
            print("skipping {} as its already completely downloaded.".format(
                project_data.project))
            continue
        batch_start_id = project_data.start_id
        batch_end_id = batch_start_id + batchsize

        while batch_start_id < project_data.end_id:
            fetch_mails_range(
                base_url=base_url,
                list_id=project_data.list_id,
                start_id=batch_start_id,
                end_id=batch_end_id)
            batch_start_id += batchsize
            batch_end_id += batchsize


def count_mails(projects_data_filename, storage_path):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        project_messages_filename = os.path.join(
            storage_path,
            project_data.list_id+'.mbox')
        mbox = mailbox.mbox(project_messages_filename)
        i = 0
        for m in mbox:
            i += 1
        print("{} contains {} mails".format(project_data.list_id, i))


if __name__ == '__main__':
    storage_path = '/home/zormit/bigdata/innovation-thesis/'
    projects_data_filename = '/home/zormit/ownCloud/Uni/msemester5/innovation-thesis/data/projects.csv'

    gmane_base_url = 'http://download.gmane.org'

    fetch_mails(projects_data_filename, gmane_base_url, storage_path)
    count_mails(projects_data_filename, storage_path)
