# -*- coding: utf-8 -*-

import sys
import mailbox
import quopri
import json
from bs4 import BeautifulSoup
import pandas as pd

MBOX = sys.argv[1]
OUT_FILE = sys.argv[2]


def cleanContent(msg):

    # Decode message from "quoted printable" format

    msg = quopri.decodestring(msg)

    # Strip out HTML tags, if any are present

    soup = BeautifulSoup(msg, "html.parser")
    return ''.join(soup.findAll(text=True))


def jsonifyMessage(msg):
    json_msg = {'parts': []}
    for (k, v) in list(msg.items()):
        json_msg[k] = v

    # The To, CC, and Bcc fields, if present, could have multiple items
    # Note that not all of these fields are necessarily defined

    for k in ['To', 'Cc', 'Bcc']:
        if not json_msg.get(k):
            continue
        json_msg[k] = json_msg[k].replace(
            '\n', '').replace(
            '\t', '').replace(
            '\r', '').replace(
            ' ', '').split(',')

    try:
        for part in msg.walk():
            json_part = {}
            if part.get_content_maintype() == 'multipart':
                continue
            json_part['contentType'] = part.get_content_type()
            content = part.get_payload(decode=False)
            json_part['content'] = cleanContent(content)

            json_msg['parts'].append(json_part)
    except Exception as e:
        sys.stderr.write('Skipping message - error encountered (%s)\n' % (str(e), ))
    finally:
        return json_msg


def compute_mails_per_id(json_msgs):
    mails_per_id = dict()
    for msg in json_msgs:
        mid = msg.get('Message-ID')
        assert mid is not None
        assert mid not in mails_per_id
        mails_per_id[mid] = msg
    return mails_per_id


def get_reply_to_root(current_id, mails_per_id):
    # TODO: dynamic programming
    while True:
        current_mail = mails_per_id.get(current_id)
        if current_mail is None:
            break
        root_id = current_id
        current_id = current_mail.get('In-Reply-To')
        if current_id is None:
            break
        assert len(current_id.split(' ')) == 1
    return root_id


def add_thread_root(mails_per_id):
    for mid, msg in mails_per_id.items():
        root = get_reply_to_root(mid, mails_per_id)
        msg['Thread-Root'] = root


# There's a lot of data to process, so use a generator to do it. See http://wiki.python.org/moin/Generators
# Using a generator requires a trivial custom encoder be passed to json for serialization of objects
class Encoder(json.JSONEncoder):

    def default(self, o):
        return list(o)


# The generator itself...
def gen_json_msgs(mb):
    for msg in mb:
        yield jsonifyMessage(msg)

mbox = mailbox.mbox(MBOX)
json_msgs = gen_json_msgs(mbox)
mails_per_id = compute_mails_per_id(json_msgs)
add_thread_root(mails_per_id)

json.dump(mails_per_id.values(), open(OUT_FILE, 'w'), indent=4, cls=Encoder)

mails = pd.DataFrame.from_dict(mails_per_id, orient='index')
# print(mails.groupby('Thread-Root').Subject.count())
