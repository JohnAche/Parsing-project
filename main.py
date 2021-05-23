'''
first parsing with multiprocessing project,
extracts data from mashina.kg (vehicle buy-sell website)
exports data to csv format
'''

import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool


count = 1 #General counter for tracking mistakes while running the program

def get_webpage_contents(url): #func gets url, extracts and returns all as text from url
    headers = {"User-Agent":"Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11"}
    response = requests.get(url, headers=headers)
    return response.text

def get_number_of_pages(webpage_contents): #func gets url, extracts and returns total number of pages
    soup = BeautifulSoup(webpage_contents, 'lxml')
    last_page_data = soup.find('div', class_='search-results-table').find('nav').find_all('li', class_='page-item')
    last_page = int(last_page_data[-1].find('a', class_='page-link').get('data-page'))
    print(f'This category has {last_page} pages of products')
    return last_page

with open('parsing_mashina.csv', 'w') as title_file: #func writes titles of columns in the file
    writer = csv.writer(title_file, delimiter=',')
    writer.writerow(('Name', 'Price_usd', 'Price_kgs', 'Image', 'Specs', 'Description'))
    
def write_to_csv(big_data): #func gets data, writes everyting to csv file
    with open('parsing_mashina.csv', 'a') as dest_file:
        writer = csv.writer(dest_file, delimiter=',')
        for data in big_data:
            try:
                writer.writerow((data['Name'],
                                data['Price_usd'],
                                data['Price_kgs'],
                                data['Image'],                             
                                data['Specs'],
                                data['Description']))
            except Exception as mistake:
                pass

def extract_webpage_data(webpage_contents): #gets webpage contents, extracts needed data
    global count
    soup = BeautifulSoup(webpage_contents, 'lxml')
    product_list = soup.find('div', class_='search-results-table').find('div', class_='table-view-list').find_all('div', class_='list-item')
    big_data = []
    for product in product_list:
        try:
            # extracting url of a stand alone ad,
            product_url =  product.find('a').get('href')
            product_url = 'https://www.mashina.kg' + product_url
            # Extracting data(soup) of a standalone car
            headers = {"User-Agent":"Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11"}
            product_response = requests.get(product_url, headers=headers).text
            product_soup = BeautifulSoup(product_response, 'lxml')
            # Extracting item name
            product_name = product_soup.find('div', class_='head-left').find('h1').text.lstrip('Продажа ')
            # Extracting product price in USD and KGS
            product_price = product_soup.find('div', class_='price-types')
            product_price_usd = product_price.find('h2').text.strip()
            product_price_kgs = product_price.find('p').text.strip()
            # Extracting product specifications
            product_specs = product_soup.find('div', class_='tab-content').find('div', class_='tab-pane').find_all('div', class_='field-row')
            # Extracting separate specs from specifications
            specifications = []
            for specs in product_specs:
                dannye = specs.text.replace('\n', '')
                specifications.append(dannye)
            # Extracting an image  of the product
            product_images = product_soup.find('div', class_='main-info').find('div', class_='fotorama').find_all('a')
            product_image = product_images[0].get('data-full')
            # Extracting product description
            product_description = product_soup.find('div', class_='seller-comments').find('h2', class_='comment').text
            descr = product_description.replace('\n', ' ')
            # Gathering collected data on item into a dictionary
            data = {
                'Name': product_name,
                'Price_usd': product_price_usd,
                'Price_kgs': product_price_kgs,
                'Image': product_image,
                'Specs': specifications,
                'Description': descr
                }
            # gathering all data collected at iteration into big_data
            big_data.append(data)
        except Exception as mistake:
            pass #print(f'{mistake} - found at iteration #' + str(count))
        count += 1
    write_to_csv(big_data)

def fast_pars(url): #the speed up func (multiprocessing)
    html = get_webpage_contents(url)
    data = extract_webpage_data(html)
    
def main(): #This func to rule them all(c)
    start = datetime.now()
    mashina_url = 'https://www.mashina.kg/search/all/'
    pages = '?page='
    total_pages = get_number_of_pages(get_webpage_contents(mashina_url))
    # total_pages = 5
    url = [mashina_url + pages + str(page) for page in range(1, total_pages + 1)]

    with Pool(80) as p: #multiprocessing start
        p.map(fast_pars, (url))

    end = datetime.now()
    print(end-start) #tracking time spent on the execution
    
if __name__ == '__main__':
    main()