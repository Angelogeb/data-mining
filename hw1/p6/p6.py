from lxml import html
import requests
import csv
import datetime 


def escape_string(s):
    return s.strip().replace("\n", " ").replace("\t", " ").replace("\r", "")

def get_announcement_from_elem(ann_elem):
    res = {}
    res["title"] = escape_string(ann_elem.find_class("title")[0].text_content())
    res["href"] = ann_elem.find_class("cta")[0].get("href")
    res["description"] = escape_string(ann_elem.find_class("description")[0].text_content())
    res["city"] = ann_elem.find_class("locale")[0].text_content()
    res["timestamp"] = ann_elem.find_class("timestamp")[0].text_content()
    return res

def get_announcements(page):
    places = page.find_class("locale")
    regular_announcements = (place.getparent() for place in places if not "topad" in place.getparent().getparent().classes)
    return [get_announcement_from_elem(elem) for elem in regular_announcements]

def get_top_announcements(page):
    places = page.find_class("locale")
    announcements = (place.getparent() for place in places)
    return [get_announcement_from_elem(elem) for elem in announcements]

def crawl_and_save(urls, get_content, writer):
    n_items = 0
    for url in urls:
        req = requests.get(url)
        content = get_content(html.fromstring(req.content))
        writer.writerows(content)
        n_items += len(content)
    return n_items

def get_last_page(url):
    req = requests.get(url)
    page = html.fromstring(req.content)
    last_page = page.find_class("last-page")
    return int(last_page[0].text_content()) if last_page != [] else 1


top_url = "https://www.kijiji.it/offerte-di-lavoro/informatica%2Cgrafica%2Cweb/?top-ads="

reg_url = "https://www.kijiji.it/offerte-di-lavoro/informatica%2Cgrafica%2Cweb/?p="


now = str(datetime.datetime.now().isoformat())
with open(now + "-announcements.tsv", "w") as f:
    top_last_page = get_last_page(top_url + str(1))
    reg_last_page = get_last_page(reg_url + str(1))

    csv_keys = ["title", "href", "city", "timestamp", "description"]

    writer = csv.DictWriter(f, csv_keys, delimiter = "\t")
    writer.writeheader()

    top_pages_urls = (top_url + str(i) for i in range(1, top_last_page + 1))
    reg_pages_urls = (reg_url + str(i) for i in range(1, reg_last_page + 1))

    n_top = crawl_and_save(top_pages_urls, get_top_announcements, writer)
    n_reg = crawl_and_save(reg_pages_urls, get_announcements, writer)

    print(top_last_page, "pages", n_top, "announcements in top announcements")
    print(reg_last_page, "pages", n_reg, "announcements in regular announcements")



