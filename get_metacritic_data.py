########################################################################################################################
# Spieldaten (Bewertung, Release, Publisher) von metacritic auslesen
# 15.09.2020
########################################################################################################################
import requests
import bs4
import json
from datetime import datetime
import os
import csv
import pandas as pd


def get_games_from_metacritic_list(from_page=0, to_page=185):
    """
    Nimmt die Metacritic "all-time-best"-Liste (von/bis einer bestimmten Seite (0-178)), sammelt die Spieletitel und die
    Platform, auf der sie bewertet wurden und gibt davon einen DataFrame aus.
    :param from_page: Ablesen der Metacriticliste von Seite x aus starten
    :param to_page: nur bis Seite x ablesen
    :return: DataFrame mit allen Spieletiteln und der zugehörigen Platform der Spiele in der abgesuchten Liste
    """
    _columns = ["title", "platform"]  # die Daten, die ausgelesen werden sollen (für die Funktion, die mehr Infos sucht)
    all_games_df = pd.DataFrame(None, columns=_columns)

    for page in range(from_page, to_page):  # nicht auf einmal die komplette Liste absuchen, sondern stückeln
        print("Scan page:", page)  # Ausgabe, wo man grade ist
        url = f"https://www.metacritic.com/browse/games/score/metascore/all/all/filtered?page={page}"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)"
                                 " AppleWebKit/537.36 (KHTML, like Gecko)"
                                 " Chrome/50.0.2661.102 Safari/537.36"}
        request = requests.get(url, headers=headers)
        if request.status_code == 200:  # falls Seite erfolgreich ausgelesen wurde
            soup = bs4.BeautifulSoup(request.text, "html.parser")
            # print(soup)
            games = soup.find_all("td", {"class": "clamp-summary-wrap"})  # Box für ein Spiel finden
            for game in games:  # für jedes Spiel die Informationen heraussuchen und aufbereiten
                # rank = game.find("span", {"class": "numbered"}).text.strip()
                title = game.find("a", {"class": "title"}).text.strip()
                platform = game.find("span", {"class": "data"}).text.strip()
                # print(rank + "\n" + title + "\n" + platform)
                all_games_df = all_games_df.append(  # Infos zum vollständigen DataFrame hinzufügen
                    pd.DataFrame({"title": [title], "platform": [platform]}), ignore_index=True)

        else:  # falls Seite nicht erfolgreich ausgelesen wurde
            print("Fehlercode:", request.status_code)
            return

    return all_games_df


def scrape_and_write_metacritic_data_of_game_to_csv(game_name, platform="pc", filepath="metacritic_game_data.csv"):
    """
    Lese die Metacriticdaten eines Spiels auf einer Platform von der Metacriticurl aus und schreibe diese Daten sortiert
    in eine csv-Datei.
    :param game_name: Name des Spiels
    :param platform: auf welcher Platform
    :param filepath: Path + Filename der csv-Outputdatei
    :return:
    """
    if game_name is None:
        return
    # platform = "pc"  # "playstation-4", "xbox-one", "switch"
    platform = platform.lower().replace(" ", "-")  # Platform für Url bereit machen
    # game_name = "Madden NFL 21"
    # game_name = "FIFA 20"
    # game_name = "Need for Speed Heat"
    game = game_name.lower().replace("#", "").replace("-", "###").replace(" ", "-")\
        .replace(":", "").replace("\'", "").replace(".", "").replace("/", "").replace(";", "").replace("&", "")\
        .replace(",", "").replace("[", "").replace("]", "").replace("$", "").replace("?", "").replace("@", "")\
        .replace("*", "")\
        .replace("--", "-").replace("###", "-")  # Spielename in Url-Name umwandeln
    url = f"https://www.metacritic.com/game/{platform}/{game}"
    print(url)
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko)"
                             " Chrome/50.0.2661.102 Safari/537.36"}
    request = requests.get(url, headers=headers)
    if request.status_code == 200:  # Seite erfolgreich abgerufen
        soup = bs4.BeautifulSoup(request.text, "html.parser")

        try:  # versuche das html-Element zu finden, in dem der Metacriticscore steht
            metacritic_score = int(soup.select("div.metascore_w.xlarge.game")[0].text)
        except IndexError or ValueError:
            metacritic_score = None
        try:  # versuche das html-Element zu finden, in dem der Userscore steht
            if soup.select("div.metascore_w.user")[0].string == "tbd":  # falls es noch nicht genug Wertungen gab
                user_score = None
            else:  # Userscore in Wert von 0-100 umwandeln
                user_score = int(float(soup.select("div.metascore_w.user")[0].string) * 10)
        except IndexError or ValueError:
            user_score = None
        info_dict = json.loads(soup.find("script", type="application/ld+json").string)  # mehr Spieldaten aus html laden
        game_info_dict = {  # Spieldaten (aufbereitet im richtigen Format) in dict schreiben
            "name": info_dict["name"],
            "metacritic": metacritic_score,
            "user": user_score,
            "description": info_dict["description"].replace("\n", " - ").replace("\r", ""),
            "genre": info_dict["genre"],
        }
        try:  # falls es ein Releasedate gibt
            game_info_dict["releasedate"] = datetime.strptime(info_dict["datePublished"], "%B %d, %Y").date()
        except ValueError:
            game_info_dict["releasedate"] = None
        try:  # falls es ein Rating gibt
            game_info_dict["rating"] = info_dict["contentRating"]
        except KeyError:
            game_info_dict["rating"] = None
        try:  # falls ein oder mehrere Publisher angegeben sind: diese in einer Liste abspeichern und zu dict hinzufügen
            publisher_full_list = info_dict["publisher"]
            publisher_name_list = []
            for publisher in publisher_full_list:
                publisher_name_list += [publisher["name"]]
        except KeyError:
            publisher_name_list = []
        game_info_dict["publisher"] = publisher_name_list
        game_info_dict["platform"] = platform

        # Spiele-dict in csv Datei mit folgenden headern schreiben
        csv_header = ["releasedate", "name", "metacritic", "user", "description", "genre", "rating", "publisher", "platform"]
        if not os.path.exists(filepath):  # Datei existiert noch nicht - header müssen noch zu erst geschrieben werden
            with open(filepath, mode="w+", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=csv_header)
                writer.writeheader()  # nur beim Anlegen der Datei in die erste Zeile schreiben
                writer.writerow(game_info_dict)  # dict als Zeile in csv schreiben
        else:  # Datei existert schon - nur eine Zeile hinzufügen
            with open(filepath, mode="a", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=csv_header)
                writer.writerow(game_info_dict)  # dict als Zeile in csv schreiben

    else:  # falls die Seite/Url aus irgendeinem Grund nicht (richtig) ausgelesen werden konnte
        print("Fehlercode:", request.status_code)
    return


def get_a_lot_of_metacritic_data(games_df):
    for _index, _row in games_df.iterrows():
        print(f"Nr. {_index}:", _row["title"])
        scrape_and_write_metacritic_data_of_game_to_csv(game_name=_row["title"], platform=_row["platform"])
    return


def read_csv_data(file="metacritic_game_data.csv"):
    """
    Liest die von Metacritic gescrapten Daten aus csv-Datei aus und gibt sie als DataFrame aus.
    :param file: Path + Dateiname der Datei, die ausgelesen werden soll
    :return: DataFrame (gebildet aus der ausgelesenen Datei und nach Releasedatum und Publisher sortiert)
    """
    csv_df = pd.read_csv(file, parse_dates=[0])  # mit pandas auslesen und die Data als Daten auslesen
    csv_df.sort_values(by=["releasedate", "publisher"], inplace=True)  # df nach releasedate und publisher sortiern

    # transform genre & publisher data to lists
    for index, row in csv_df.iterrows():
        row["genre"] = str(row["genre"]).strip("][").replace("\'", "").split(", ")
        row["publisher"] = str(row["publisher"]).strip("][").replace("\'", "").split(", ")
    return csv_df


def new_avg_of_game_on_different_platforms(full_df):
    df_unique = full_df.drop_duplicates(["name"], keep="first")
    for game in df_unique["name"]:
        df_one_game = full_df.loc[full_df["name"] == game]
        df_unique.loc[df_unique["name"] == game, "metacritic"] = round(df_one_game["metacritic"].mean(), 2)
    return df_unique


def filter_df(publisher, platform, from_file="metacritic_game_data.csv"):
    """
    Filtere aus den Metacriticdaten alle Spiele eines Publishers heraus.
    :param publisher: Publishername(n) als Liste
    :param platform: Platform(en) als Touple
    :param from_file: aus welcher Datei sollen die Daten ausgelesen werden
    :return: nach angegebenen Parametern gefilterter DataFrame
    """
    full_df = read_csv_data(from_file)
    full_df.sort_values(["publisher", "releasedate"], inplace=True)

    columns = ["releasedate", "name", "metacritic", "user", "description", "genre", "rating", "publisher", "platform"]
    filtered_df = pd.DataFrame(None, columns=columns)
    for index, row in full_df.iterrows():
        # transform genre & publisher data to lists
        row["genre"] = str(row["genre"]).strip("][").replace("\'", "").split(", ")
        row["publisher"] = str(row["publisher"]).strip("][").replace("\'", "").split(", ")

        if publisher and platform:  # nach Publisher und Platform filtern
            if any(_x in row["publisher"] for _x in publisher) and any(_x in [row["platform"]] for _x in platform):
                filtered_df = filtered_df.append(row, ignore_index=True)
        elif publisher and not platform:  # nur nach Publisher
            if any(_x in row["publisher"] for _x in publisher):
                filtered_df = filtered_df.append(row, ignore_index=True)
        elif not publisher and platform:  # nur nach Platform filtern
            if any(_x in [row["platform"]] for _x in platform):
                filtered_df = filtered_df.append(row, ignore_index=True)
        else:  # es wurde nichts gefiltert
            filtered_df = full_df
    filtered_df.sort_values("releasedate", inplace=True)

    filtered_df = new_avg_of_game_on_different_platforms(filtered_df)

    return filtered_df


if __name__ == "__main__":
    # df = get_games_from_metacritic_list()  # quasi alle Spiele, die es auf Metacritic gibt in einen df schreiben
    # df.to_csv("gamelist.csv")  # als csv abspeichern
    #
    # df = pd.read_csv("gamelist.csv")  # alle Spiele-csv laden
    # get_a_lot_of_metacritic_data(df)  # metacritic infos zu den Spielen sammeln --> metacritic_game_data.csv

    # df = read_csv_data()
    df = filter_df(["Electronic Arts", "EA Sports", "EA Games"], None)  # publisher=[], platform=("pc", )
    print(df)
