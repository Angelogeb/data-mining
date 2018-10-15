import argparse
from lxml import html
import requests
import csv
import datetime 
from multiprocessing import Process

parser = argparse.ArgumentParser(description='Scrape kijiji.it Informatica/Grafica/Web category')
parser.add_argument('-f','--full_desc', help='Download full description', action='store_true')
parser.add_argument('-n', '--n_proc', help='Number of processes to run', type=int, default = 6)
args = parser.parse_args()


csv_keys = ["title", "href", "city", "timestamp", "description"]
if args.full_desc:
    csv_keys.append("full_description")

now = str(datetime.datetime.now().isoformat()) + ("-full_desc" if args.full_desc else "")
file_name = now + "-announcements.tsv"

top_url = "https://www.kijiji.it/offerte-di-lavoro/offerta/informatica-e-web/?top-ads="

reg_url = "https://www.kijiji.it/offerte-di-lavoro/offerta/informatica-e-web/?p="

def escape_string(s):
    return s.strip().replace("\n", " ").replace("\t", " ").replace("\r", "")

def get_announcement_from_elem(ann_elem):
    res = {}
    res["title"] = escape_string(ann_elem.find_class("title")[0].text_content())
    res["href"] = ann_elem.find_class("cta")[0].get("href")
    res["description"] = escape_string(ann_elem.find_class("description")[0].text_content())
    res["city"] = ann_elem.find_class("locale")[0].text_content()
    res["timestamp"] = ann_elem.find_class("timestamp")[0].text_content()

    if args.full_desc:
        full_desc_elem = html.fromstring(requests.get(res["href"])).find_class("vip__text-description")
        if full_desc_elem != []:
            res["full_description"] = full_desc_elem[0].text_content()

    return res

def get_announcements(page):
    places = page.find_class("locale")
    regular_announcements = (place.getparent() for place in places if not "topad" in place.getparent().getparent().classes)
    return [get_announcement_from_elem(elem) for elem in regular_announcements]

def get_top_announcements(page):
    places = page.find_class("locale")
    announcements = (place.getparent() for place in places if not "extended-result" in place.getparent().getparent().classes)
    return [get_announcement_from_elem(elem) for elem in announcements]

def crawl_and_save(urls, get_content, writer):
    n_items = 0
    for url in urls:
        req = requests.get(url)
        content = get_content(html.fromstring(req.content))
        writer.writerows(content)
        n_items += len(content)
    print(n_items, " Retrieved")
    return n_items

def get_last_page(url):
    req = requests.get(url)
    page = html.fromstring(req.content)
    last_page = page.find_class("last-page")
    return int(last_page[0].text_content()) if last_page != [] else 1


top_last_page = get_last_page(top_url + str(1))
top_pages_urls = [top_url + str(i) for i in range(1, top_last_page + 1)]


with open("top-"+file_name, "w") as f:

    writer = csv.DictWriter(f, csv_keys, delimiter = "\t")
    writer.writeheader()

    n_top = crawl_and_save(top_pages_urls, get_top_announcements, writer)

    print(top_last_page, "pages", n_top, "announcements in top announcements")


reg_last_page = get_last_page(reg_url + str(1))
reg_pages_urls = [reg_url + str(i) for i in range(1, reg_last_page + 1)]

print("reg_last_page =", reg_last_page)

def split_in(k, l):
    rem = len(l) % k
    list_len = len(l) // k
    if (rem != 0): list_len += 1
    return [l[i * list_len : (i+1) * list_len] for i in range(k)]

urls_partitions = split_in(args.n_proc, reg_pages_urls)

ps = []
for (i, par) in enumerate(urls_partitions):
    f = open(str(i) + "-" + file_name, "w")
    writer = csv.DictWriter(f, csv_keys, delimiter = "\t")
    # writer.writeheader()
    p = Process(target = crawl_and_save, args =(par, get_announcements, writer))
    p.start()
    ps.append((p, f))

for (p, f) in ps:
    p.join()
    f.close()