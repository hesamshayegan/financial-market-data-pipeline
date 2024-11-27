import pandas as pd
import yfinance as yf
import os



# get the directory of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

# get the parent directory
parent_dir = os.path.dirname(current_dir)

# construct the input file's path
file_path = os.path.join(parent_dir, 'input', 'stock_symbols.csv')

input_file = pd.read_csv(file_path)

stocks = input_file['stock_symbol'].tolist()


def get_stock_history():
    all_stocks = []
    for stock_symbol in stocks:
        stock = yf.Ticker(stock_symbol)
        try:
            df = stock.history(start="2014-01-01", end=None)
            if not df.empty:    
                df['stock'] = stock_symbol
                df.index = df.index.tz_localize(None)
                df = df.reset_index()
                # drop all na values
                # df = df.dropna()
                all_stocks.append(df)
                print(f"Stock history for {stock_symbol} was successfully extracted")
            else:
                print(f"No history data for {stock_symbol}")

        except Exception as e:
            print(f"Error retrieving data for {stock_symbol}: {e}")
                
    if all_stocks:
        combined = pd.concat(all_stocks, ignore_index=True)
        return combined[['Date',
                        'Open',
                        'High',
                        'Low',
                        'Close',
                        'Volume',
                        'stock']]
    
    else:
        print("No data available for the provided stocks.")
        return pd.DataFrame()
    


def get_yearly_income():
    all_stocks_income = []
    for stock_symbol in stocks:  
        stock = yf.Ticker(stock_symbol)

        try:
            df = stock.income_stmt.transpose()
            if not df.empty:
                df['stock'] = stock_symbol
                # drop all na values
                # df = df.dropna()
                all_stocks_income.append(df)
            else:
                print(f"No income data for {stock_symbol}")
        except Exception as e:
            print(f"Error retrieving data for {stock_symbol}: {e}")
    
    if all_stocks_income:
        # Combine all DataFrames, aligning on all dates (outer join)
        combined = pd.concat(all_stocks_income, axis=0, join='outer').reset_index()
        # Rename columns for clarity
        combined.rename(columns={'index': 'Date'}, inplace=True)
        return combined[['Date', 
                        'Gross Profit', 
                        'Total Revenue',
                        'Diluted EPS',
                        'stock']]
    else:
        print("No data available for the provided stocks.")
        return pd.DataFrame()



# get quartely income 
def get_quarterly_income():
    all_stocks_income = []
    for stock_symbol in stocks:  
        stock = yf.Ticker(stock_symbol)

        try:
            df = stock.quarterly_income_stmt.transpose()
            if not df.empty:
                df['stock'] = stock_symbol
                # drop all na values
                # df = df.dropna()
                all_stocks_income.append(df)
            else:
                print(f"No income data for {stock_symbol}")
        except Exception as e:
            print(f"Error retrieving data for {stock_symbol}: {e}")
    
    if all_stocks_income:
        # Combine all DataFrames, aligning on all dates (outer join)
        combined = pd.concat(all_stocks_income, axis=0, join='outer').reset_index()
        # Rename columns for clarity
        combined.rename(columns={'index': 'Date'}, inplace=True)
        return combined[['Date', 
                        'Gross Profit', 
                        'Total Revenue',
                        'stock']]
    else:
        print("No data available for the provided stocks.")
        return pd.DataFrame()