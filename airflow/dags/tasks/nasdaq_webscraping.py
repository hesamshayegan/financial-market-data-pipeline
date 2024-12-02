from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime


def check_stock_symbol(stocks):
    valid_symbols = []
    for stock in stocks:
        url = f"https://www.google.com/finance/quote/{stock}:NASDAQ"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        try:
            if soup.find(class_="eYanAe") != None:
                valid_symbols.append(stock)
        except:
            continue
    
    return valid_symbols
      

statistics = []

def get_stock_statistics(stocks):

    valid_symbols = check_stock_symbol(stocks)

    if not valid_symbols:
        print("The list is empty")
        return None
    
    id = 0
    for stock in valid_symbols:
        url = f"https://www.google.com/finance/quote/{stock}:NASDAQ"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:

            soup = BeautifulSoup(response.content, 'html.parser')

            market_cap = 0
            pe = 0
            dividend = 0
            

            stats_table = soup.find(class_="eYanAe")

            for row in stats_table.find_all('div'):
                if len(row.find_all('div')) > 2:
                    current_time = datetime.datetime.now()
                    label = row.find_all('div')[0].text
                    value = row.find_all('div')[2].text
                    if row.find_all('div')[0].text == 'Market cap':
                        market_cap = str(value)    
                    elif row.find_all('div')[0].text == 'P/E ratio':
                        pe = str(value)
                        
                    elif label == 'Dividend yield':
                        dividend = str(value) if value != '-' else '0'
            
            id += 1

            statistics.append({"ID": id, "Date": current_time, "Market Cap": market_cap, "PEratio": pe, "Dividend": dividend, "Stock": stock})

        else:
            print(f"Error fetching data for {stock}")

    statistics_df = pd.DataFrame(statistics)

    return statistics_df[["ID", "Date", "Market Cap", "PEratio", "Dividend", "Stock"]]
