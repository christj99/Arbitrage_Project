import json
from sys import argv
import requests
import pandas as pd
from pprint import pprint
import gspread
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials


# The goal of this project is to gather sportsbook odds from an api, and caclulate what abritrage bets are possible between all of the bookmakers and sports
# the results of this project are saved to a csv file and uploaded to a google drive sheet
# all of the code below runs as intended and the above goals are met however there are still additional steps i would take before calling it complete. 


#The user gives what they would like the total revenue to be from the bet
winnings = int(input())

API_KEY = #Your API KEY

#SPORT = 'americanfootball_ncaaf' # use the sport_key from the /sports endpoint below, or use 'upcoming' to see the next 8 games across all sports

REGIONS = 'us' # uk | us | eu | au. Multiple can be specified if comma delimited

MARKETS = 'spreads,totals,h2h' # h2h | spreads | totals. Multiple can be specified if comma delimited

ODDS_FORMAT = 'decimal' # decimal | american

DATE_FORMAT = 'iso' # iso | unix

BOOKMAKERS = 'draftkings'

#nested dictionaries in the json file, this code makes it such that each key in the nested dictionaries recieves its own column
def expand_books(bookmake):
    book_exp = pd.DataFrame(columns = ['id','key', 'title', 'last_update', 'market'])
    for row_num in range(len(bookmake)):
        row = bookmake.iloc[row_num]
        for tester in row['bookmakers']:
            for bet in range(len(tester['markets'])):
                cede = pd.DataFrame([{'id':row['id'],'key':tester['key'], 'title':tester['title'], 'last_update':tester['last_update'],'market':tester['markets'][bet]['key']}])
                #now that the nested dictionaries have been flattened, this adds extra columns for the different prices based on the odds given.                 
                prices = tester['markets'][bet]['outcomes']
                outcos = []
                for price in range(len(prices)):
                    outco = pd.DataFrame([prices[price].values()], columns = [col+str(price+1) for col in prices[price].keys()])
                    outcos.append(outco)
                full_out = pd.concat(outcos, axis = 1)
                booking = pd.concat([cede, full_out], axis=1)
                book_exp = pd.concat([book_exp, booking], axis = 0)
    return book_exp

def arbitrage(full_up, winnings):
    arb_full = pd.DataFrame()
    ids = full_up['id'].unique()
    for id in ids:
        for market in MARKETS.split(','):
            # matching game ids to the markets specified in the markets variable
            books = full_up[(full_up['id'] == id) & (full_up['market'] == market)]
            for bk in range(len(books)):
                # calculating arbitrage odds and wager for all games for all bookmakers
                book = books.iloc[[bk]]
                odds1 = book['price1']
                w1 = winnings/odds1.iloc[0]
                o1_wager = pd.DataFrame([{'wager_1': w1}])
                min_odd = 1/(1-(1/odds1))
                # isolating which games and books' odds are capable of an arbitrage bet
                arb_books = books[books['price2']>min_odd.iloc[0]]
                # adding what the second wager would need to be for each game for each book to the dataframe, based on the first set of odds, to have an arbitrage bet
                compare = {(arb_books.iloc[booknum]['title']+' wager 2'):(winnings/arb_books.iloc[booknum]['price2']) for booknum in range(len(arb_books))}
                bets = {item[1]:item[0] for item in list(compare.items())}
                # calculating which book for which game gives the bets arbitrage profit and adding it to the dataframe
                if len(bets) != 0:
                    optimum = min(list(bets.keys()))
                    best = {"Best Bet":(bets[optimum],optimum,str(round((winnings-(w1+optimum))/winnings,4)*100)+'%')}
                    best_bet = pd.DataFrame([best])
                else:
                    best_bet=pd.DataFrame([{"Best Bet":"None"}])
                full_comp = pd.DataFrame([compare])
                #Joining the wagers and full dataframe of possible arbitrage bets to the dataframe of books and odds for all the games. 
                wager = pd.concat([o1_wager,full_comp,best_bet], axis = 1)
                wager.index = book.index 
                total_row = book.join(wager)
                arb_full = pd.concat([arb_full, total_row], axis = 0, ignore_index=True)

    return arb_full

def main():

    # Connecting to the google sheet with my google credentials

    path = 'client_secret.json'
    gc = pygsheets.authorize(service_account_file=path)
    sh = gc.open('Arbys')
    sheets = sh.worksheets(force_fetch=True)
    sheet_lst = [sheet.title for sheet in sheets]

    # gather the parameters for the api

    API_KEY = #your API key here
    #SPORT = 'americanfootball_ncaaf' # use the sport_key from the /sports endpoint below, or use 'upcoming' to see the next 8 games across all sports
    REGIONS = 'us' # uk | us | eu | au. Multiple can be specified if comma delimited
    MARKETS = 'spreads,totals,h2h' # h2h | spreads | totals. Multiple can be specified if comma delimited
    ODDS_FORMAT = 'decimal' # decimal | american
    DATE_FORMAT = 'iso' # iso | unix

    # gather list of sports to get odds from

    sports_res = requests.get(f'https://api.the-odds-api.com/v4/sports/?apiKey={API_KEY}')
    s = sports_res.json()
    sdf = pd.DataFrame.from_dict(s)
    sports = sdf[sdf['has_outrights'] == False]
    sports_dict = {sports.iloc[row]['key']:sports.iloc[row]['title'] for row in range(len(sports))}
    sports_lst = list(sports_dict.keys())

    #get odds from api for each sport

    for num in range(len(sports_lst)):
        SPORT = sports_lst[num]
        print(num, SPORT)

        odds_response = requests.get(
        f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds',
        params={
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
            'dateFormat': DATE_FORMAT,
            }
        )
        
        j = odds_response.json()

        # Put odds in dataframe 
        df = pd.DataFrame.from_dict(j)
        #print(df.columns)
        games_id = ['id','sport_key','sport_title','commence_time','home_team','away_team']
        games = df.loc[:,games_id]
        bookmake = df.loc[:,['id','bookmakers']]

        # flatten nested dictionaries

        full_book = expand_books(bookmake)

        full_up = pd.merge(games,full_book, on='id')

        arb_full = arbitrage(full_up, winnings)

        if num == 0:
            total_arb = arb_full
        else:
            total_arb = pd.concat([arb_full, total_arb], ignore_index=True)

        # put sports odds in a google sheet

        if sports_dict[SPORT] in sheet_lst:
            sp_sht = sh.worksheet(property = 'title', value = sports_dict[SPORT])
            sh.del_worksheet(sp_sht)

        wksht=sh.add_worksheet(sports_dict[SPORT],rows=arb_full.shape[0],cols=arb_full.shape[1])
        wksht.set_dataframe(arb_full,(1,1))
    
    sports = list(sports_dict.values())
    sheets = sh.worksheets(force_fetch=True)
    sheet_lst = [sheet.title for sheet in sheets]

    # delete sports with outdated odds

    for sheet in sheet_lst:
        if not sheet in sports:
            sp_sht = sh.worksheet(property = 'title', value = sheet)
            sh.del_worksheet(sp_sht)


    #csv file for backup
    #total_arb.to_csv("total.csv")

main()