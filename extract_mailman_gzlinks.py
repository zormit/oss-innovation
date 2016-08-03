with open('../data/https:_www.monetdb.org_pipermail_developers-list-txt-gz-links') as html_links:
    for line in html_links:
        start = line.find("https")
        end = line.find("txt.gz") + len("txt.gz")
        print(line[start:end])
