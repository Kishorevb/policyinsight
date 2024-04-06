import os
import sys
import requests
from bs4 import BeautifulSoup
import hashlib
import json
import psycopg2
import newspaper
from datetime import datetime
from urllib.parse import urljoin
import time
import random
from requests.auth import HTTPProxyAuth
from multiprocessing import Process, Queue
import multiprocessing
from multiprocessing import Pool
import warnings
warnings.filterwarnings("ignore")


def read_config(config_file):
    with open(config_file, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

def extract_breadcrumbs(response_text, website_url):
    try:
     soup = BeautifulSoup(response_text, 'html.parser')
     breadcrumb_container = None

     # Identify the breadcrumb container based on HTML structure
     if "canada.ca" in website_url:
         breadcrumb_container = soup.find('nav', {'property': 'breadcrumb'})
     elif "ftc.gov" in website_url:
         breadcrumb_container = soup.find('nav', {'aria-labelledby': 'system-breadcrumb'})
     elif "fda.gov" in website_url:
         breadcrumb_container = soup.find('ol', class_='lcds-breadcrumb')

     if breadcrumb_container:
         # Extract breadcrumb items
         breadcrumb_items = breadcrumb_container.find_all('li')
         if len(breadcrumb_items) > 2:
             return breadcrumb_items[1].get_text(strip=True), breadcrumb_items[2].get_text(strip=True)
         elif len(breadcrumb_items) > 1:
             return breadcrumb_items[1].get_text(strip=True), 'None'
         elif len(breadcrumb_items) == 1:
             return "Category Home", 'None'
     else:
         return "Root", 'None'
    except requests.exceptions.RequestException as e:
     print(f"Error fetching links from {url}: {e}")
     return "no category"
    except Exception as e:
     print(f"Error extracting breadcrumbs for url {url}: {e}")
     pass 


def get_links_from_page(url):
    try:
        proxies = {
            'http': 'xxxxxx'
        }
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Google-InspectionTool/1.0;)"}

        response = requests.get(url, proxies=proxies, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.find_all('a', href=True)]
        return links, response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching links from {url}: {e}")
        return []
    except Exception as e:
        print(f"Error extracting links from page for url {url}: {e}")
        pass

def extract_date_modified(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        date_modified_element = soup.find('dt', text='Date modified:')
        if date_modified_element:
            date_modified_value = date_modified_element.find_next('dd').get_text(strip=True)
            if date_modified_value == "":
                return datetime.now().isoformat()
            return date_modified_value
        else:
            return datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error extracting date modified: {e}")
        pass
    return None

def download_html(url, response, postgres_conn, website_url):
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get the article title
        article_title = soup.title.get_text() if soup.title else ""

        # Define allowed tags
        allowed_tags = ['p', 'h1', 'h2', 'h3', 'li', 'ul']

        # Extract text content of allowed tags and concatenate them
        article_text = '\n'.join(tag.get_text(separator='\n') for tag in soup.find_all(allowed_tags))

        # Calculate the hash
        content_hash = hashlib.sha256(article_text.encode('utf-8')).hexdigest()

        # Extract date modified
        date_modified = extract_date_modified(response.text)
        
        url_category, url_subcategory = extract_breadcrumbs(response.text, website_url)

        # Check if the URL is already in the database
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute("SELECT title, content, date_modified, hash FROM website_entries WHERE url = %s", (url,))
        existing_data = postgres_cursor.fetchone()
        postgres_cursor.close()

        old_title, old_content, old_date_modified, old_hash = None, None, None, None
        if existing_data:
            old_title, old_content, old_date_modified, old_hash = existing_data
        

        # Create or Update to PostgreSQL based on the URL availability and content modification        
        if existing_data and (article_title != old_title or content_hash != old_hash):
            print(f"Changes detected in: {url} with title: {article_title}")
            postgres_cursor = postgres_conn.cursor()
            #Write the change set to database for UI updates
            #Commenting for not to 
            '''
            postgres_cursor.execute("""
                INSERT INTO website_updates (url, title, url_category, url_subcategory, previous_page_content, previous_modified_date, updated_page_content, updated_modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (url, old_title, url_category, url_subcategory, old_content, old_date_modified, article_text, date_modified))'''     
            
            
            #Archive the old data
            postgres_cursor.execute("""
                INSERT INTO website_archives (url, title, url_category, url_subcategory, archive_date, content, date_modified)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (url, old_title, url_category, url_subcategory, datetime.now(), old_content, date_modified))
            postgres_cursor.execute("""
                UPDATE website_entries SET (title, url_category, url_subcategory, content, hash, date_modified) = (%s, %s, %s, %s, %s, %s)
                WHERE url = %s
            """, (article_title, url_category, url_subcategory, article_text, content_hash, date_modified, url))
            postgres_conn.commit()
            postgres_cursor.close()
            print(f"Updated: {url}")
        elif existing_data is None:
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute("""
                INSERT INTO website_entries (url, title, url_category, url_subcategory, content, hash, date_modified)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (url, article_title, url_category, url_subcategory, article_text, content_hash, date_modified))
            postgres_conn.commit()
            postgres_cursor.close()

            print(f"Downloaded: {url} (Title: {article_title}, URL Category: {url_category}, URL Subcategory: {url_subcategory})")
        else:
            print(f"Content not changed for: {url} with title: {article_title}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading HTML from {url}: {e}")
        pass
    except Exception as e:
        print(f"Error downloading html for {url}: {e}")
        pass
                
def consume_urls(config_file, url_queue, sleep_interval):
    print("A consumer started...", flush=True)
    
    # Read application configuration from the config file
    config = read_config(config_file)

    # PostgreSQL database connection information
    postgres_host = config["postgres"]["host"]
    postgres_port = config["postgres"]["port"]
    postgres_user = config["postgres"]["user"]
    postgres_password = config["postgres"]["password"]
    postgres_database = config["postgres"]["database"]

    # Connect to PostgreSQL
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        database=postgres_database
    )
    
    timeout = 300 #seconds
    timeout_start = time.time()
    while time.time() < timeout_start + timeout:
        url, processed, response = url_queue.get()
        url_queue.put((url, True, None))
        if not processed:            
            timeout_start = time.time()
            download_html(url, response, postgres_conn, config["website_url"])
        time.sleep((random.randint(0, sleep_interval) / 1000))
    postgres_conn.commit()
    postgres_cursor.close()
    print("A consumer has completed successfully...", flush=True)
            
def produce_urls(root_url, url_queue, max_depth, blacklist_url_substrings, blacklist_url_extensions, sleep_interval):
    print("Producer starts...", flush=True)
    visited_urls = set()
    urls_list = [(root_url, 0)]
    url_queue.put((root_url, False))

    urls_count = 0
    while urls_list:
        random.shuffle(urls_list)
        try:
            current_url, depth = urls_list.pop(0)

            if current_url in visited_urls or \
                depth > max_depth or \
                any(substr in current_url for substr in [x.strip() for x in blacklist_url_substrings.split(',')]) or \
                current_url.endswith(tuple([x.strip() for x in blacklist_url_extensions.split(',')])):
                continue

            links, response = get_links_from_page(current_url)
            visited_urls.add(current_url)
            urls_count += 1
            print(f"{datetime.now().strftime('%d-%m-%Y %H:%M:%S')} URL #{urls_count} : {current_url}", flush=True)              
            url_queue.put((current_url, False, response))

            for link in links:
                absolute_link = urljoin(current_url, link)

                if absolute_link.startswith(root_url) and absolute_link not in visited_urls:
                    urls_list.append((absolute_link, depth + 1))
            time.sleep((random.randint(0, sleep_interval) / 1000))
        except Exception as e:
            print(f"Error downloading links for {current_url}: {e}")
            pass
    print("Producer has completed successfully...", flush=True)


result_list = []
def log_result(result):
    # This is called whenever foo_pool(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    result_list.append(result)
    
                
# Main
if __name__ == "__main__":
    print("Webcrawler started...")
    input_arguments = sys.argv
    if len(input_arguments) < 2:
        print("Incorrect application usage: python webcrawler.py config_file.json")
        exit()
        
    # Read application configuration from the config file
    config = read_config(input_arguments[1])

    # PostgreSQL database connection information
    postgres_host = config["postgres"]["host"]
    postgres_port = config["postgres"]["port"]
    postgres_user = config["postgres"]["user"]
    postgres_password = config["postgres"]["password"]
    postgres_database = config["postgres"]["database"]

    # Connect to PostgreSQL
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        database=postgres_database
    )

    # Create tables in PostgreSQL if not exists
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS website_entries (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            url_category TEXT,
            url_subcategory TEXT,        
            content TEXT NOT NULL,
            hash TEXT NOT NULL,
            date_modified TEXT
        )
    """)

    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS website_archives (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            url_category TEXT,
            url_subcategory TEXT,        
            archive_date TIMESTAMP NOT NULL,
            content TEXT NOT NULL,
            date_modified TEXT
        )
    """)

    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS website_updates (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            url_category TEXT,
            url_subcategory TEXT,
            previous_page_content TEXT,
            previous_modified_date TEXT NOT NULL,
            updated_page_content TEXT,
            updated_modified_date TEXT NOT NULL
        )
    """)
    postgres_conn.commit()
    postgres_cursor.close()

    #Todo -- Max depth config needs to be figured out and updated
    max_depth = config["max_depth"]

    #Root URL to be crawled for checking udpates
    website_url = config["website_url"]
    print(website_url)

    #Sleep interval in milliseconds between consecutive URL requests to website
    sleep_interval = config["sleep_interval_in_msec"]
    blacklist_url_substrings = config["blacklist_url_substrings"]
    blacklist_url_extensions = config["blacklist_url_extensions"]

    # Create a Queue for communication between processes
    m = multiprocessing.Manager()
    url_queue = m.Queue()
    
    # Create the producer process
    pool = Pool(processes=os.cpu_count())
    async_producer_results = pool.apply_async(produce_urls, args=(website_url, url_queue, max_depth, blacklist_url_substrings, blacklist_url_extensions, sleep_interval))
    for i in range(os.cpu_count() - 1):
        pool.apply_async(consume_urls, args=(input_arguments[1], url_queue, sleep_interval), callback = log_result)
    results = [async_producer_results.get()]
    results.extend(result_list)
    
    pool.close()
    pool.join()
    print("Webcrawler has completed successfully...")