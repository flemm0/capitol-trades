from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import json
import datetime
import boto3


def lambda_handler(event, context) -> None:
    week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).date()
    # set up Chrome driver
    print('Settting up chrome driver...')
    chrome_options = Options()
    chrome_service = Service('/opt/chromedriver')
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--single-process')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options, service=chrome_service)
    print('Chrome driver set up successfully!')
    # begin webscraping
    print('Beginning webscraping...')
    page, data, end = 1, [], False
    while not end:
        driver.get(f'https://www.capitoltrades.com/trades?per_page=96&page={page}')
        time.sleep(5)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        rows = soup.find('table', {'class': 'q-table trades-table'}).find('tbody').find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            published_date = cols[2].text.strip()
            if 'Today' in published_date:
                published_date = datetime.datetime.today().date().strftime('%Y %d %b')
            elif 'Yesterday' in published_date:
                published_date = (datetime.datetime.today() - datetime.timedelta(days=1)).date().strftime('%Y %d %b')
            published_date_obj = datetime.datetime.strptime(published_date, '%Y %d %b').date()
            if published_date_obj <= week_ago:
                end = True
                break
            party, chamber, state = tuple([ele.text for ele in cols[0].find_all(class_='q-field')])
            record = {
                'politician': cols[0].find(class_='q-fieldset politician-name').text,
                'party': party,
                'chamber': chamber,
                'state': state,
                'issuer_name': cols[1].find(class_='q-fieldset issuer-name').text,
                'issuer_ticker': cols[1].find(class_='q-field issuer-ticker').text,
                'published_date': published_date,
                'traded_date': cols[3].text.strip(),
                'filed_after': cols[4].text.strip(),
                'owner': cols[5].text.strip(),
                'type': cols[6].text.strip(),
                'size': cols[7].text.strip(),
                'price': cols[9].text.strip()
            }
            data.append(record)
        page += 1
    print('Webscrape complete!')
    print('Writing data to S3...')
    fname = f'trades_{'_'.join([datetime.datetime.today().strftime('%d-%m-%Y'), week_ago.strftime('%d-%m-%Y')])}.json'
    s3 = boto3.client('s3')
    json_data = json.dumps(data)
    s3.put_object(
        Bucket='capitol-trades-data-lake',
        Key=f'trades/{fname}',
        Body=json_data
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Data scraped and uploaded to S3 successfully!')
    }

