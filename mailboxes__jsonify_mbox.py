# -*- coding: utf-8 -*-

import sys
import mailbox
import quopri
import json
from bs4 import BeautifulSoup

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
json.dump(gen_json_msgs(mbox), open(OUT_FILE, 'w'), indent=4, cls=Encoder)
