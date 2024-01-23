print("Проверка на пропущенные номера игры.")
import pandas as pd
import get_data
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor

data_dvazhdydva = pd.read_csv("data_dvazhdydva.csv", sep=";")
print(data_dvazhdydva.head())

data_dvazhdydva_comb = pd.read_csv("data_dvazhdydva_comb.csv", sep=";")
print(data_dvazhdydva_comb.head())

data_duel = pd.read_csv("data_duel.csv", sep=";")
print(data_duel.head(10))

data_duel_comb = pd.read_csv("data_duel_comb.csv", sep=";")
print(data_duel_comb.head(10))


lst = list(data_dvazhdydva[data_dvazhdydva['super_prize_rub'].isnull()].number_game)

data_dvazhdydva = data_dvazhdydva.query("number_game not in @lst")
data_dvazhdydva.to_csv("data_dvazhdydva.csv", sep=";", index=False)

data_dvazhdydva_comb = data_dvazhdydva_comb.query("number_game not in @lst")
data_dvazhdydva_comb.to_csv("data_dvazhdydva_comb.csv", sep=";", index=False)

del lst

lst = list(data_duel[data_duel['super_prize_rub'].isnull()].number_game)[9:]

data_duel = data_duel.query("number_game not in @lst")
data_duel.to_csv("data_duel.csv", sep=";", index=False)

data_duel_comb = data_duel_comb.query("number_game not in @lst")
data_duel_comb.to_csv("data_duel_comb.csv", sep=";", index=False)

del lst


numbers_game_dd = set(range(data_dvazhdydva.number_game.min(), data_dvazhdydva.number_game.max()+1))
no_numbers_dd = list(numbers_game_dd - set(data_dvazhdydva.number_game))
print(f"no_numbers_dd {len(no_numbers_dd)/len(set(data_dvazhdydva.number_game)):.2%}", len(no_numbers_dd))

numbers_game_dl = set(range(data_duel.number_game.min(), data_duel.number_game.max()+1))
no_numbers_duel = list(numbers_game_dl - set(data_duel.number_game))
print(f"no_numbers_duel {len(no_numbers_duel)/len(set(data_duel.number_game)):.2%}", len(no_numbers_duel))

def worker(game_number):
    d, dc = get_data.get_data(game_number)
    if d is not None:
        get_data.data.append(d)
        get_data.data_comb.extend(dc)

for url, numbers  in {"https://www.stoloto.ru/dvazhdydva/archive": no_numbers_dd,
                     "https://www.stoloto.ru/duel/archive": no_numbers_duel}.items():
    if numbers:
        get_data.use_url(url)
        l = len(numbers)
        print(url, f"has {l} no_numbers")
        for index in range(0, l, 1000):
            time_start = dt.now().replace(microsecond=0)
            stop = index + 1000 if l - index > 1000 else l
            get_data.url = url
            get_data.puf = url.split('/')[-2]
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(worker, numbers[index:stop])
            get_data.data.sort()
            get_data.data_comb.sort()
            get_data.data_to_csv(get_data.data, get_data.data_comb)
            get_data.data.clear()
            get_data.data_comb.clear()
            time_stop = dt.now().replace(microsecond=0)
            time = time_stop-time_start
            print(f"{stop-index} row write {time}. {time_stop.time()}")
            
        print(url, "END")
    else:
        print(url, "has not no_numbers")
else:
    print("END")


__import__("os").system('pause')
