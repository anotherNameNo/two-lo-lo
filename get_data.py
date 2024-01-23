# get_data_v2.0_Thread 

#pip install beautifulsoup4
#pip install requests
#pip install lxml

from unicodedata import normalize
from datetime import datetime as dt
import re
import csv
from time import sleep as sleep_time


from bs4 import BeautifulSoup
import requests
from threading import Thread

#url = "https://www.stoloto.ru/dvazhdydva/archive"
#url = "https://www.stoloto.ru/duel/archive"


data, data_comb = [], []



columns_data = ["number_game", "date", "l_1", "l_2", "r_1", "r_2",
             "all_tickets", "combination",
               "totat_winnings_rub", "super_prize_rub",]

columns_data_comb = ["number_game", "guessed_nambers", "winning_comb",
                    "winning_rub", "amount_winnings_rub"]

fake_headers =  {'User-Agent':
                    "Mozilla/5.0 (X11; Linux x86_64)AppleWebKit/537.36 "+\
                    "(KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}


def use_url(url_in="https://www.stoloto.ru/dvazhdydva/archive"):
    global url
    url = url_in
    puf = url.split('/')[-2]
    global files
    files = {"data": f"data_{puf}.csv",
         "data_comb": f"data_{puf}_comb.csv",
         "last": f"last_number_{puf}.txt"}
    
    

def get_soup(url: str, sleep:int = 0.1, attempts:int=9, head=False):
    func = {False: requests.get, True: requests.head}
    for attempt in range(1, 1+attempts):
        try:
            page = func[head](url, headers=fake_headers, timeout=(2,5))
            if page.status_code == 200:
                # for headers use page.headers
                return BeautifulSoup(page.text, "lxml")
            continue
        except:
            time_sleep = sleep
            continue
    else:
        print(f"No url: {url}")
            

def get_start_game_number(start:int=0 , stop:int=1000 )->int:
    """The function finds the minimum number of the game and returns it."""
    
    if start == stop: return  start
    if start > stop: start, stop = stop, start
        
    median_number = (stop - start) // 2 + start
    if get_soup(f"{url}/{median_number}", head=True):
        return get_start_game_number(start, median_number)
    else:
        return get_start_game_number(median_number+1, stop)     
    

def datestring_to_iso(date:str)->str:
    """Функция переводит нераспознаваемую автоматически строку
    с датой в ISO формат."""

    months = {"января": "01", "февраля": "02", "марта": "03", "апреля": "04",
              "мая": "05", "июня": "06", "июля": "07", "августа": "08",
              "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",}

    for month in months:
        if month in date:
            date = date.replace(month, months[month])
            break

    date = dt.strptime(date, '%d %m %Y в %H:%M')
    return date.strftime("%Y-%m-%dT%H:%M")


def data_to_csv(data:list, data_comb:list)->None:
    flag = __import__("os").path.isfile(files["data"])
    
    with open (files["data"], "a", encoding="UTF-8") as file_data,\
        open (files["data_comb"], "a", encoding="UTF-8") as file_data_comb:
        #lineterminator default /r/n
        file_writer_data = csv.writer(file_data,
                                      delimiter=";", lineterminator="\r")
        file_writer_data_comb = csv.writer(file_data_comb,
                                      delimiter=";", lineterminator="\r")
        if not flag:
            file_writer_data.writerow(columns_data)
            file_writer_data_comb.writerow(columns_data_comb)
            
        [file_writer_data.writerow(i) for i in data]
        [file_writer_data_comb.writerow(i) for i in data_comb]


def get_data(game_number):
    soup = get_soup(f"{url}/{game_number}")
    if soup is None: return None, None
    
    # get date game
    pattern_date = r"[1-3]{0,1}[0-9]{1} \w*? \d{4}.*"
    datetime_of_game = re.search(pattern_date, soup.find("h1").text)[0]
    datetime_of_game = datestring_to_iso(datetime_of_game)

    # get 4 number
    numbers = soup.find("div", {"class": "winning_numbers cleared"}).text.split()
    
    #get table result
    pattern_result = r"[0-9][0-9 .,]*"
    result_table_text = normalize('NFKD',
                              soup.find("div",{"class": "details_data"}).text)
    result_table = re.findall(pattern_result, result_table_text)
    
    result_table = [i.replace(" ", "") for i in result_table]
    
    while len(result_table)<4:
        result_table.insert(1, "0")
    

    data = ([game_number] + [datetime_of_game] + numbers + result_table)

    #get table comb
    pattern_comb = r"([0-9] \+ [0-9])\s(\d+)\s(\d+)\s(\d+)"
    combs = re.findall(pattern_comb, soup.find("div",{"class": "inner"}).text)
    combs = [[game_number] + list(comb) for comb in combs]
    
    return data, combs


def worker(*iterable):
    for game_number in iterable:
        d, dc = get_data(game_number)
        if d is not None:
            data.append(d)
            data_comb.extend(dc)


def main(thrs=10, step=100):
    start_time = dt.now().replace(microsecond=0)
    print(f"Start time {start_time.time()}")

    use_url(url_in="https://www.stoloto.ru/dvazhdydva/archive")
    soup = get_soup(url)
    if soup is None:
        print(f"Program end.")
        __import__("sys").exit(0)
        __import__("os").system('pause')

    current_game_number = int(soup.find("div", {"class": "elem"}).find("a").text)

    try:
        df = __import__("pandas").read_csv(files["data"], sep=";")
        start_game_number = df.number_game.max() + 1
    except:
        print("No Start game number, start get_start_game_number")
        start_game_number = get_start_game_number(stop=current_game_number)
    
    print(f"Start game number {start_game_number}")
    print(f"Current game number {current_game_number}")

    for game_number in range(start_game_number,
                             current_game_number+1,
                             thrs*step):

        data.clear()
        data_comb.clear()
        
        time_start = dt.now().replace(microsecond=0)
        workers = []
        
        for thr in range(thrs):
            start = game_number+thr*step
            stop = game_number+(thr+1)*step
            if stop > current_game_number+1: stop = current_game_number+1
            work = Thread(target=worker, name=f"worker_{thr}",
                         args=range(start, stop))
            workers.append(work)
            if stop == current_game_number+1: break

        [work.start()  for work in workers]
        [work.join()  for work in workers]

        data.sort()
        data_comb.sort()
        data_to_csv(data, data_comb)
        time_stop = dt.now().replace(microsecond=0)
        time = time_stop-time_start
        
        print(f"{data[-1][0]}: {len(data)} row...{time}, {time_stop.time()}")
    
    print("END", dt.now().replace(microsecond=0).time())
    __import__("os").system('pause')

    
if __name__ == "__main__":
    main(int(input("Threads=\n")), int(input("step=\n")))


