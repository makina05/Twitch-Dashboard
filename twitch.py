import requests
import json
import mysql.connector
import time
import datetime


# Conseguimos la info para la API
client_id = 'st7qx1bdxrxjxaieam2alqqhfvug3c'
client_secret = 'l1sfouwc93i7v7f9ncu8xp4rnma630'

access_token_request = requests.post('https://id.twitch.tv/oauth2/token?client_id='
                                     + str(client_id) + '&client_secret='
                                     + str(client_secret) + '&grant_type=client_credentials')

access_token = json.loads(access_token_request.text)['access_token']

headers = {
    'Client-ID': client_id,
    'Authorization': "Bearer " + str(access_token),
}

# conexion con MySQL

connection = mysql.connector.connect(
    host="localhost", user="xabid", password="1234", database="twitchdata")
mycursor = connection.cursor()


def topGames():

    # consigue info sobre top10 de juegos

    top10_games_request = requests.get(
        'https://api.twitch.tv/helix/games/top?first=10', headers=headers)

    top10_games_data = json.loads(top10_games_request.text)['data']

    # guardamos los id y nombre de cada juego en una lista

    top10_games = []
    for game in top10_games_data:
        top10_games.append((game['id'], game['name']))

    # conseguimos viewers por juego

    top10_stream = []
    for game in top10_games:
        game_viewers_request = requests.get(
            'https://api.twitch.tv/helix/streams?game_id=' + game[0] + '&first=100', headers=headers)
        game_viewers = json.loads(game_viewers_request.text)['data']

        # hacer la suma de todos los viewers por juego
        viewers = 0
        for stream in game_viewers:
            viewers += stream['viewer_count']
        top10_stream.append((game[0], game[1], viewers))

    # borrar toda la info de la db antes de meter nueva info
    mycursor.execute("DELETE FROM topgames")

    # guarda la info en la db
    for stream in top10_stream:
        mycursor.execute("INSERT INTO topgames VALUES('" +
                         stream[0] + "', '" + stream[1] + "','" + str(stream[2]) + "')")

    connection.commit()


def topStreamers():

    # consigue info sobre los streamers
    top_streamers_request = requests.get(
        'https://api.twitch.tv/helix/streams?first=3', headers=headers)
    top_streamers = json.loads(top_streamers_request.text)['data']

    # vamos a guardar el nombre del streamer, sus viewers y la hora
    streamer_info = []

    # borra los datos de hace mas de 6 horas (asi no se acumulan tantos) 
    mycursor.execute("DELETE FROM topstreamers WHERE date < '" + str(datetime.datetime.now() - datetime.timedelta(minutes = 360 )) + "'")

    for streamer in top_streamers:
        streamer_info.append(
            (streamer['user_name'], streamer['viewer_count'], datetime.datetime.now()))

    for each in streamer_info:
        mycursor.execute("INSERT INTO topstreamers VALUES('" +
                         each[0] + "', '" + str(each[1]) + "','" + str(each[2]) + "')")

    connection.commit()


def totalViewers():

    # viewers en ingles

    # conseguimos los datos de los streams que sean en ingles
    top_en_request = requests.get(
        'https://api.twitch.tv/helix/streams?lenguage=en&first=100', headers=headers)
    top_en = json.loads(top_en_request.text)['data']
    en_viewers = 0

    mycursor.execute("DELETE FROM idiomas")

    # se calcula el numero de viewers en ingles
    for en_stream in top_en:
        en_viewers += en_stream['viewer_count']

    mycursor.execute(
        "INSERT INTO idiomas VALUES('English','" + str(en_viewers) + "')")

    # viewers en español

    # conseguimos los datos de los streams que sean en español
    top_es_request = requests.get(
        'https://api.twitch.tv/helix/streams?lenguage=es&first=100', headers=headers)
    top_es = json.loads(top_es_request.text)['data']
    es_viewers = 0

    # se calcula el numero de viewers en español
    for es_stream in top_en:
        es_viewers += en_stream['viewer_count']

    mycursor.execute(
        "INSERT INTO idiomas VALUES('Spanish','" + str(es_viewers) + "')")

    # viewers en frances

    # conseguimos info de los streams en frances
    top_fr_request = requests.get(
        'https://api.twitch.tv/helix/streams?lenguage=fr&first=100', headers=headers)
    top_fr = json.loads(top_fr_request.text)['data']
    fr_viewers = 0

    # calculamos numero de viewers en frances
    for fr_stream in top_fr:
        fr_viewers += fr_stream['viewer_count']

    mycursor.execute(
        "INSERT INTO idiomas VALUES('French','" + str(fr_viewers) + "')")

    connection.commit()


while True:

    topGames()
    topStreamers()
    totalViewers()
    time.sleep(60)
    # se consiguen nuevos datos cada minuto
