import requests
import re
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import socket
import json
import ipaddress
import logging
import coloredlogs
import html
import pycurl
from io import BytesIO
import time
from io import StringIO
from datetime import datetime, timedelta
import traceback

SITES_TO_IGNORE = [
    'https://cla.microsoft.com',
    'https://opensource.microsoft.com',
    'https://www.typescriptlang.org',
    'https://marketplace.visualstudio.com',
    'https://microsoft.com',
    'https://duendesoftware.com/products/identityserver',
    'https://app.vssps.visualstudio.com',
    'https://login.microsoftonline.com',
    'https://review.docs.microsoft.com',
    'https://amcdn.msftauth.net',
    'https://msrc.microsoft.com',
    'https://ms.gallery.vsassets.io',
    'https://spsprodcus3.vssps.visualstudio.com',
    'https://github.co/hiddenchars',
    'https://azuresdkartifacts.blob.core.windows.net',
    'https://code.visualstudio.com',
    'https://feedback.azure.com',
    'https://dotnet.microsoft.com',
    'https://www.nuget.org',
    'https://forms.office.com',
    'https://shell.azure.com',
    'https://www.powershellgallery.com',
    'https://msdn.microsoft.com',
    'https://cla.opensource.microsoft.com',
    'https://visualstudio.com',
    'https://eng.ms/docs',
    'https://spsprodeus27.vssps',
    'https://itrellis.gallery.vsassets.io',
    'https://about.ads.microsoft.com',
    'https: // azuremarketplace.microsoft',
    'https://www.linkedin.com',
]

proxy_credentials = [
['http://45.146.31.116:5703', 'iknuqlkt', '1deyizkkwpdz'],
['http://5.157.130.181:8185', 'iknuqlkt', '1deyizkkwpdz'],
['http://173.239.237.245:5891', 'iknuqlkt', '1deyizkkwpdz'],
['http://91.217.72.226:6955', 'iknuqlkt', '1deyizkkwpdz'],
['http://102.212.88.227:6224', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.43.90.76:6591', 'iknuqlkt', '1deyizkkwpdz'],
['http://150.107.225.203:6468', 'iknuqlkt', '1deyizkkwpdz'],
['http://204.44.69.239:6492', 'iknuqlkt', '1deyizkkwpdz'],
['http://103.80.10.25:6303', 'iknuqlkt', '1deyizkkwpdz'],
['http://154.85.124.235:6096', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.79.84:5998', 'iknuqlkt', '1deyizkkwpdz'],
['http://216.173.122.234:5961', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.73.14:5102', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.239.3.230:6190', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.239.52.173:7335', 'iknuqlkt', '1deyizkkwpdz'],
['http://184.174.28.59:5074', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.65.140:6819', 'iknuqlkt', '1deyizkkwpdz'],
['http://103.3.227.211:6764', 'iknuqlkt', '1deyizkkwpdz'],
['http://45.43.71.207:6805', 'iknuqlkt', '1deyizkkwpdz'],
['http://45.43.190.233:6751', 'iknuqlkt', '1deyizkkwpdz'],
['http://91.246.195.168:6937', 'iknuqlkt', '1deyizkkwpdz'],
['http://45.56.174.1:6254', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.70.127:5678', 'iknuqlkt', '1deyizkkwpdz'],
['http://167.160.180.110:6661', 'iknuqlkt', '1deyizkkwpdz'],
['http://154.92.125.208:5509', 'iknuqlkt', '1deyizkkwpdz'],
['http://216.173.107.252:6220', 'iknuqlkt', '1deyizkkwpdz'],
['http://45.41.178.141:6362', 'iknuqlkt', '1deyizkkwpdz'],
['http://154.29.235.122:6463', 'iknuqlkt', '1deyizkkwpdz'],
['http://154.194.10.34:6047', 'iknuqlkt', '1deyizkkwpdz'],
['http://38.154.206.161:9652', 'iknuqlkt', '1deyizkkwpdz'],
['http://172.245.157.134:6719', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.238.10.114:6060', 'iknuqlkt', '1deyizkkwpdz'],
['http://171.22.248.78:5970', 'iknuqlkt', '1deyizkkwpdz'],
['http://185.102.49.125:6463', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.59.66:6659', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.143.226.239:5842', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.250.201.219:6764', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.239.43.26:5754', 'iknuqlkt', '1deyizkkwpdz'],
['http://142.111.93.64:6625', 'iknuqlkt', '1deyizkkwpdz'],
['http://206.206.64.224:6185', 'iknuqlkt', '1deyizkkwpdz'],
['http://66.78.32.45:5095', 'iknuqlkt', '1deyizkkwpdz'],
['http://154.30.242.2:9396', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.239.38.197:6730', 'iknuqlkt', '1deyizkkwpdz'],
['http://84.33.241.149:6506', 'iknuqlkt', '1deyizkkwpdz'],
['http://103.101.88.170:5894', 'iknuqlkt', '1deyizkkwpdz'],
['http://206.232.13.54:5720', 'iknuqlkt', '1deyizkkwpdz'],
['http://142.111.93.252:6813', 'iknuqlkt', '1deyizkkwpdz'],
['http://216.10.27.95:6773', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.64.127.179:6132', 'iknuqlkt', '1deyizkkwpdz'],
['http://64.137.57.19:6028', 'iknuqlkt', '1deyizkkwpdz'],
['http://38.170.176.145:5540', 'iknuqlkt', '1deyizkkwpdz'],
['http://104.239.53.48:7466', 'iknuqlkt', '1deyizkkwpdz'],
['http://185.245.25.192:6453', 'iknuqlkt', '1deyizkkwpdz'],
['http://23.229.110.130:8658', 'iknuqlkt', '1deyizkkwpdz'],
]


visited = set()



BASE_CRAWELIG_URL = "https://github.com/Azure"

requests_counter = 0

# Create a logger object
logger = logging.getLogger(__name__)
now = datetime.now()
log_filename = now.strftime('%Y-%m-%d_%H-%M-%S.log')
coloredlogs.install(level='DEBUG', logger=logger)
file_handler = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# We don't want to crawel the same link more than once
examined_links = BASE_CRAWELIG_URL

resolved_addresses = {}
start_time = time.time()
"""def crawl(url, parent_url, level):
    child_links = []
    links = fetch(url, parent_url)
    for link in links:
        if should_crawl(link):
            for sublink in fetch(link, url):
                fetch(sublink, url)
        else:
            pass
"""

def crawl(url, max_depth):
    to_visit = [(url, '', 0)]
    # TODO - when do we stop?
    proxies = get_proxy()
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = []
        # Run in a loop and take 100 urls each time.
        while True:
            for future in list(futures):
                if future.done():
                    if future.exception() != None:
                        logger.error(str(future.exception()))
                    to_visit = to_visit + future.result()

                    futures.remove(future)
                    # NEED TO NOW REMOVE IT
            urls_to_check = to_visit[:1]
            for url in urls_to_check:
                visited.add(url[0])
            del to_visit[:len(urls_to_check)]
            # Sometimes the list is empty
            if len(urls_to_check) == 0:
                print("List is empty. Waiting 5 sec")
                time.sleep(5)
                continue
            else:
                futures.append(executor.submit(crawl_1_urls, urls_to_check, proxies, max_depth))
                ### NEED TO CHECK HERE THAT WE DO NOT ENTER NEW LINKS WE HAVE LOOKED INTO !!!!
                ## !!!!!!
                ## NEED TO ALSO ENTER THE NEW VISITED URLS!
                #to_visit.extend((child, url,depth + 1) for child in child_urls)


def crawl_1_urls(urls, proxies, max_depth):
    child_urls = []
    proxy = next(proxies)
    new_urls = []
    while len(urls) != 0:
        url, parent_url, depth = urls.pop(0)
        if depth > max_depth:
            continue

        done = False
        tries = 0
        while (not done and tries != 15):
            # if fail 3 times, go to the next url
            try:
                new_urls = fetch(url, parent_url, proxy)
                done = True
            except Exception as e:
                tries = tries + 1
                #print("Failed to fetch")
                if tries == 10:
                    logger.error("Tried to fetch 15 times. " + str(e) + " - Failed to query " + url + " ; proxy - " + proxy[0])
                    logger.error(traceback.format_exc())
                # Sometime the proxy fail. If that is that case than we want to try again with a different proxy
                if proxy[0].split('http://')[1].split(':')[0] in str(e):
                    logger.debug("Proxy problem with " + proxy[0] + ". Changing proxy. " + str(e))
                    # Change to new proxy if we have a problem.
                    proxy = next(proxies)
                else:
                    # In case of random error we do not want to stop
                    proxy = next(proxies)
        for new_url in new_urls:
            # Only append if this is in Azure's github or it is an Azure IP
            if should_crawl(new_url,url):
                child_urls.append([new_url, url, depth + 1])

    return child_urls

def fetch_url(url, parent_url, proxy):
    global requests_counter
    if requests_counter % 500 == 0:
        running_time = time.time() - start_time
        logger.info("Running time - " + str(running_time) + " ; Requests - " + str(requests_counter) + " ; Requests per mintue - " + str(requests_counter / (running_time / 60)))
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(c.HEADER, 1)

    def header_callback(header_line):
        # decode and write the header line to string buffer
        headers.write(header_line.decode('iso-8859-1'))

    headers = StringIO()
    #if url.startswith("https://github.com"):
    #    c.setopt(c.HTTPHEADER, ['Authorization: Bearer ' + github_token])
    c.setopt(c.HEADERFUNCTION, header_callback)
    c.setopt(c.SSL_VERIFYPEER, 0)  # equivalent to verify_ssl=False in requests
    c.setopt(c.TIMEOUT, 10)  # equivalent to timeout=10 in requests
    c.setopt(c.FOLLOWLOCATION, True) # Allow redirects

    proxy_address = proxy[0]
    proxy_user = proxy[1]
    proxy_password = proxy[2]
    #import pdb;pdb.set_trace()
    c.setopt(c.PROXY, proxy_address)
    c.setopt(c.PROXYUSERPWD, proxy_user + ':' + proxy_password)
    c.perform()
    # Getting HTTP response code
    response_code = c.getinfo(pycurl.HTTP_CODE)
    content_type = c.getinfo(pycurl.CONTENT_TYPE)
    c.close()
    #logger.info('Response ' + str(response_code) + ' url - ' + url + "\nparent - " + parent_url)
    return (response_code, content_type, buffer)

def fetch(url, parent_url, proxy):
    #import pdb;pdb.set_trace()
    global requests_counter
    requests_counter += 1
    response_code = 429
    while response_code == 429:
        response_code, content_type, buffer =  fetch_url(url, parent_url, proxy)

        if response_code == 429:
            print("Got 429. Sleeping 5 seconds")
            time.sleep(5)
    if not url.startswith("https://github.com/Azure"):
        logger.debug(("Fetched URL; Response code: " + str(response_code) + " ; URL: " + url + " ; Parent - " + parent_url))
        pass
    else:
        print("Fetched URL; Response code: " + str(response_code) + " ; URL: " + url + " ; Parent - " + parent_url)
        pass
    if response_code == 404:
        return ''
    # We don't want to scan images for new urls
    elif content_type != None and content_type.startswith("image"):
        return ''
    res_text = buffer.getvalue().decode('utf-8')
    links = find_links(res_text, url)
    return links

def find_links(response, link):
    try:
        data = response.replace('\n', '')
        #import pdb;pdb.set_trace()
        if 'GetUrlContents' in data or "https://azuresdkartifacts.blob.core.windows.net/azure-sdk-write-teams/" in data:
            logger.info("GetUrlContents or azure-sdk-write-teams in URL - " + link)
        # Find normal URLs that include http/s
        url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[!@$%^&*+?:;`{\|}[\]~//\/=\-_.])+')
        encode_urls = re.findall(url_pattern, data)
        # For HTTP pages - find href that does not inclue http/s
        href_pattern = re.compile(r'href=[\'"]?([^\'" >]+)')
        matches = href_pattern.findall(response)
        hostname = urlparse(link).hostname
        for match in matches:
            if not match.startswith("http") and not match.startswith("mailto:"):
                url = link.split('//')[0] + "//" + hostname + match
                #logger.critical("Adding - " + url)
                encode_urls.append(url)
        # Sometimes we see catch HTML entities which we would want to decode
        decoded_urls = [html.unescape(url) for url in encode_urls]
    except Exception as e:
        print(e)
        import pdb;pdb.set_trace()
    # We would want to ignore some files as they won't have relevant data, for example, pictures
    return remove_urls(filter_urls(decoded_urls))

def remove_urls(links):
    return [link for link in links if not any(link.startswith(prefix) for prefix in SITES_TO_IGNORE)]

def filter_urls(url_list):
    # Now that the urls are HTML decoded, we need to check that they are indeed valid and we didn't mistakes.
    invalid_chars = ['"', '\'']
    extensions_to_ignore = [".png", ".ico", ".jpg" , ".jpeg", ".gif", ".tar", ".zip", ".gz", ".png?raw=true", '..']
    valid_urls = [url for url in url_list if not any(char in url for char in invalid_chars)]
    urls =  [url for url in valid_urls if not any(url.endswith(ext) for ext in extensions_to_ignore)]
    return urls

def should_crawl(link, parent):
    # Is the link in Azure GitHub or the ip is in the Azure IP ranges?
    if link.startswith('https://github.com/Azure') or is_server_in_azure(link, azure_subnets, parent):
        # Add the links we have examined
        if link not in visited:
            return True
    return False

def url_to_ip(hostname):
    # Debug if needed
    if hostname == '' or hostname == None:
        return None
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        return ''
    except UnicodeError:
        return None
    return ip

def get_azure_subnets():
    logger.info('Downloading the azure subnets list')
    url = 'https://download.microsoft.com/download/7/1/D/71D86715-5596-4529-9B13-DA13A5DE5B63/ServiceTags_Public_20240415.json'  # URL of the file to be downloaded
    response = requests.get(url, allow_redirects=True, verify=False)
    data = json.loads(response.text)
    subnets = []
    for value in data['values']:
        subnets.extend(value['properties']['addressPrefixes'])

        # Function to check if an IP is in one of the subnets
    return subnets

def is_server_in_azure(link, subnets, parent):
    try:
        hostname = urlparse(link).hostname
    except Exception:
        logger.error("Could not convert link to hostname. Link - " + link)
        return False
    if hostname == None:
        logger.error("Could not convert link to hostname. Link - " + link)
        return False
    if hostname in resolved_addresses:
        return resolved_addresses[hostname]
    ip = url_to_ip(hostname)
    if ip == None:
        logger.error("Could not convert hostname to ip. Hostname -  " + hostname + "; link - " + link)
        return False
    try:
        ip = ipaddress.ip_address(ip)
    except ValueError:
        return False

    for subnet in subnets:
        if ip in ipaddress.ip_network(subnet):
            resolved_addresses[hostname] = True
            return True
    resolved_addresses[hostname] = False
    return False

def get_proxy():
    while True:
        for proxy in proxy_credentials:
            yield proxy

if __name__ == '__main__':
    azure_subnets = get_azure_subnets()
    crawl(BASE_CRAWELIG_URL, 20)
