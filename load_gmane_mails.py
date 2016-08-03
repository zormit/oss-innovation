import os
import pickle

import pandas as pd
import requests
import mailbox
import nntplib


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


def transform_to_mboxo(projects_data_filename, storage_path, from_line):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        raw_project_messages_filename = os.path.join(
            storage_path,
            'raw',
            project_data.list_id+'.mbox')
        mboxo_project_messages_filename = os.path.join(
            storage_path,
            'mboxo',
            project_data.list_id+'.mbox')

        with open(raw_project_messages_filename, 'r') as raw_msgs:
            with open(mboxo_project_messages_filename, 'w') as mboxo_msgs:
                for line in raw_msgs:
                    if line.startswith('From ') and line[5:].strip() != from_line:
                        mboxo_msgs.write('>'+line)
                    else:
                        mboxo_msgs.write(line)


def transform_to_mboxo_generic(projects_data_filename, storage_path, from_line):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        raw_project_messages_filename = os.path.join(
            storage_path,
            'raw',
            project_data.list_id+'.mbox')
        mboxo_project_messages_filename = os.path.join(
            storage_path,
            'mboxo',
            project_data.list_id+'.mbox')

        with open(raw_project_messages_filename, 'r') as raw_msgs:
            with open(mboxo_project_messages_filename, 'w') as mboxo_msgs:
                previous_line = None
                for line in raw_msgs:
                    if previous_line is not None:
                        if previous_line.startswith('From '):
                            if line.startswith('From:'):
                                # valid From
                                mboxo_msgs.write(previous_line)
                            else:
                                # invalid From
                                mboxo_msgs.write('>'+previous_line)
                                print(previous_line)
                        else:
                            # normal line
                            mboxo_msgs.write(previous_line)
                    previous_line = line


def extract_headers_only(projects_data_filename, storage_path):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        mboxo_project_messages_filename = os.path.join(
            storage_path,
            'mboxo',
            project_data.list_id+'.mbox')
        headers_project_messages_filename = os.path.join(
            storage_path,
            'header',
            project_data.list_id+'.pkl')

        mbox = mailbox.mbox(mboxo_project_messages_filename)
        headers = []
        for m in mbox:
            headers.append(dict(m))

        with open(headers_project_messages_filename, 'wb') as headers_dump:
            pickle.dump(headers, headers_dump)


def count_mails(projects_data_filename, storage_path, from_line):
    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for row_id, project_data in projects_data.iterrows():
        project_messages_filename = os.path.join(
            storage_path,
            'mboxo',
            project_data.list_id+'.mbox')
        mbox = mailbox.mbox(project_messages_filename)
        i = 0
        for m in mbox:
            i += 1
            assert m.get_from() == from_line
        print("{} contains {} mails".format(project_data.list_id, i))


def fetch_gmane_ids(projects_data_filename, gmane_news_url, storage_path):
    news_server = nntplib.NNTP(gmane_news_url)

    projects_data = pd.read_csv(projects_data_filename, skipfooter=1, engine='python')
    for _, project_data in projects_data.iterrows():
        project_overview_filename = os.path.join(
            storage_path,
            'nntp_overview',
            project_data.list_id+'.pkl')

        print('connecting to {}'.format(project_data.list_id))
        resp, _, _, _, _ = news_server.group(project_data.list_id)
        print(resp)
        print('fetching overviews...')
        resp, overviews = news_server.over((project_data.start_id, project_data.end_id))
        print(resp)
        with open(project_overview_filename, 'wb') as overviews_dump:
            pickle.dump(overviews, overviews_dump)

    news_server.quit()


if __name__ == '__main__':
    storage_path = '/home/zormit/bigdata/innovation-thesis/'
    projects_data_filename = '/home/zormit/ownCloud/Uni/msemester5/innovation-thesis/data/projects.csv'

    gmane_base_url = 'http://download.gmane.org'
    gmane_news_url = 'news.gmane.org'
    gmane_from_line = 'news@gmane.org Tue Mar 04 03:33:20 2003'

    fetch_mails(projects_data_filename, gmane_base_url, storage_path)
    count_mails(projects_data_filename, storage_path, gmane_from_line)
    transform_to_mboxo(projects_data_filename, storage_path, gmane_from_line)
    transform_to_mboxo_generic(projects_data_filename, storage_path, gmane_from_line)
    extract_headers_only(projects_data_filename, storage_path)
    fetch_gmane_ids(projects_data_filename, gmane_news_url, storage_path)
