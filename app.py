# import everything
import re
from flask import Flask, request
import telegram
from captain_digibot.credentials import bot_token, bot_user_name,URL

import pandas as pd
import json
import requests
from SVY21 import *
from numpy import sqrt 

def get_hdb_carpark():
    
    df = pd.DataFrame()
    
    for n in range(1000):
    
        url = 'https://data.gov.sg/api/action/datastore_search?offset=' + str(n*100) + '&resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c'
        res = requests.get(url)

        if res.json()['result']['records'] != []:
            for k in res.json()['result']['records']:
                t_dic = {}
                for i,v in k.items():
                    t_dic[i] = [v]

                df_new_row = pd.DataFrame(t_dic)  
                df = pd.concat([df, df_new_row])
                df['x_coord'] = df['x_coord'].astype(float)
                df['y_coord'] = df['y_coord'].astype(float)
        else:
            break
    return df

def get_cp_availability():

    t_dic = {}
    df = pd.DataFrame()

    url = 'https://api.data.gov.sg/v1/transport/carpark-availability'
    res = requests.get(url)

    for k in res.json()['items'][0]['carpark_data']:
        for i,v in k.items():
            if i =='carpark_info':
                for i2,v2 in v[0].items():
                    t_dic[i2] = [v2]
            else:
                t_dic[i] = [v]

        df_new_row = pd.DataFrame(t_dic)  
        df = pd.concat([df, df_new_row])

    df.shape
    
    return df

def convert_svy21(n,e):
    cv = SVY21()

    # Computing Lat/Lon from SVY21
    lon, lat = cv.computeLatLon(n, e)
    return(lon,lat)

def convert_merge(df):
    t_df = pd.DataFrame()
    
    for row in df.iterrows():
        t_dic = {}

        lat, lon = convert_svy21(row[1]['y_coord'],row[1]['x_coord'])
        t_dic['car_park_no'] = [row[1]['car_park_no']]
        t_dic['lat'] = [lat]
        t_dic['lon'] = [lon]
    
        df_new_row = pd.DataFrame(t_dic)
        t_df = pd.concat([t_df, df_new_row])
        
    merged_df = df.merge(t_df, on='car_park_no', how='left')
        
    return merged_df

def get_destination_lat_lon(postalcode):
    
    url = 'https://developers.onemap.sg/commonapi/search?searchVal=' + str(postalcode) + '&returnGeom=Y&getAddrDetails=Y&pageNum=1'

    res = requests.get(url)
    lat = res.json()['results'][0]['LATITUDE']
    lon = res.json()['results'][0]['LONGTITUDE']

    return lat, lon

def distance_between(x1, y1, x2, y2):
    return sqrt((x1-x2)**2 + (y1-y2)**2)



global bot
global TOKEN
TOKEN = bot_token
bot = telegram.Bot(token=TOKEN)

# start the flask app
app = Flask(__name__)

@app.route('/{}'.format(TOKEN), methods=['POST'])
def respond():
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram.Update.de_json(request.get_json(force=True), bot)

    chat_id = update.message.chat.id
    msg_id = update.message.message_id

    # Telegram understands UTF-8, so encode text for unicode compatibility
    text = update.message.text.encode('utf-8').decode()
    # for debugging purposes only
    print("got text message :", text)
    # the first time you chat with the bot AKA the welcoming message
    if text == "/start":
        # print the welcoming message
        bot_welcome = """
        Welcome to HDB carpark locator. Please enter your destination postal code and it will fetch you the 5 nearest HDB parking and number of available slots
        """
        # send the welcoming message
        bot.sendMessage(chat_id=chat_id, text=bot_welcome, reply_to_message_id=msg_id)


    else:
        try:
            df = get_hdb_carpark()
            merged_df = convert_merge(df)
            lat, lon = get_destination_lat_lon(238164)
            
            df2 = pd.DataFrame()

            for row in merged_df.iterrows():
                t_dic = {}
                dist = distance_between(float(lat), float(lon), float(row[1]['lat']), float(row[1]['lon']))

                t_dic['car_park_no'] = [row[1]['car_park_no']]
                t_dic['lat'] = [row[1]['lat']]
                t_dic['lon'] = [row[1]['lon']]
                t_dic['dist'] = [dist]
                
                df_new_row = pd.DataFrame(t_dic)
                df2 = pd.concat([df2, df_new_row])

            merged_df = merged_df.merge(df2, on='car_park_no', how='left')

            merged_df = merged_df.sort_values(by='dist', ascending=True)

            avail = get_cp_availability()

            ref = pd.DataFrame()

            for cp in merged_df.iterrows():
                
                temp_df = avail[(avail['carpark_number'] == cp[1]['car_park_no'])]
                ref = ref.append(temp_df)

            final_df = ref.merge(merged_df, left_on='carpark_number', right_on='car_park_no')

            counter = 0
            for i in final_df.iterrows():
                print(i[1]['carpark_number'])
                print(i[1]['address'])
                print(i[1]['lots_available'])
                print(i[1]['update_datetime'])
                print("==========")
                counter = counter + 1
                if counter == 5:
                    break
            
            textcompile = "this is working uat"

            # clear the message we got from any non alphabets
            ##text = re.sub(r"\W", "_", text)
            # create the api link for the avatar based on http://avatars.adorable.io/
            ##url = "https://api.adorable.io/avatars/285/{}.png".format(text.strip())
            # reply with a photo to the name the user sent,
            # note that you can send photos by url and telegram will fetch it for you
            bot.sendPhoto(chat_id=chat_id, text=textcompile, reply_to_message_id=msg_id)
        except Exception:
            # if things went wrong
            bot.sendMessage(chat_id=chat_id, text="Exception error. Please ensure you have entered a valid 6 digit postal code", reply_to_message_id=msg_id)

        return 'ok'


@app.route('/setwebhook', methods=['GET', 'POST'])
def set_webhook():
    # we use the bot object to link the bot to our app which live
    # in the link provided by URL
    s = bot.setWebhook('{URL}{HOOK}'.format(URL=URL, HOOK=TOKEN))
    # something to let us know things work
    if s:
        return "webhook setup ok"
    else:
        return "webhook setup failed"

@app.route('/')
def index():
    return '.'


if __name__ == '__main__':
    # note the threaded arg which allow
    # your app to have more than one thread
    app.run(threaded=True)