from bs4 import BeautifulSoup
import requests
from collections import defaultdict
import re
import csv


__author__ = 'frederikpietzko'

with open('key_words.txt', encoding='utf-8') as f:
    key_words = f.readlines()

city_identifications = []
with open('zuordnung_plz_ort.csv', encoding='utf-8') as f:
    lines = f.readlines()
    for i in range(1, len(lines)):
        city_identifications.append(lines[i].split(',')[0])

data = dict()
regex = re.compile(r'[\W_]+', re.UNICODE)


def crawl_page(html_file):
    soup = BeautifulSoup(html_file, 'lxml')
    matches = soup.find_all('div', class_='hit clearfix ')

    companies = set()
    for match in matches:
        name = match.find('a', class_='name')
        name = regex.sub(' ',name.text) if name is not None else ''
        address = match.find('address')
        address = regex.sub(' ',address.text )if address is not None else ''
        tel = match.find('div', class_='right')
        tel = int(regex.sub(' ',''.join(list(filter(lambda x: x.isdigit(), tel.text))) ))if tel is not None else ''
        data_set = (name, address, tel)
        companies.add(data_set)

    return list(companies)


def generate_url(key_word, city_identification):
    return 'https://www.dasoertliche.de/Controller?zvo_ok=0&choose=true&page=0&context=0&action=43&topKw=0&form_name=search_nat&kw=' + key_word + '&ci=' + str(city_identification)


def compute_response(url):
    html_file = requests.get(url).text.encode(errors='ignore')
    soup = BeautifulSoup(html_file, 'lxml')
    pages = soup.find('div', class_='paging').find_all('a')  # get href for next page

    refs = [page.get('href') for page in pages if not page.has_attr('class') and page.name is not 'span']

    html_files = [requests.get(ref).text for ref in refs]

    html_files.insert(0, html_file)

    companies = []
    for html_file in html_files:
        companies += crawl_page(html_file)

    gathered = {}
    company_dict = defaultdict(list)
    for name, address, tel in companies:
        for n, a in gathered.keys():
            s_name, s_address, s_n, s_a = set(name), set(address), set(n), set(a)
            name_comparison = s_name.issubset(s_n) or s_n.issubset(s_name)
            address_comparison = s_address.issubset(s_a) or s_a.issubset(s_address)
            if name_comparison and address_comparison:
                company_dict[gathered[n,a]].append(tel)
                gathered[name,address] = gathered[n,a]
                break
        else:
            company_dict[name, address].append(tel)
            gathered[name,address] = name, address

    companies = [(name, address, company_dict[name, address]) for name, address in company_dict.keys()]

    return companies


def yield_ratios(s, iterable):
    for x in iterable:
        s.set_seq1(x)
        yield s.ratio()


for key_word in key_words:
    for city_identification in city_identifications:
        data[key_word, city_identification] = compute_response(generate_url(key_word, city_identification))
        if len(data) % 1 == 10000:
            with open('data.csv', mode='w+', encoding='utf-8') as f:
                csv_writer = csv.writer(f)
                for val in data.values():
                    for ra in val:
                        csv_writer.writerow(ra)




