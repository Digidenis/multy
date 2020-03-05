# from websocket import create_connection
import datetime
import websocket
import json
import pika
import time


def throw_to_rabbit(full_message):
    """
    отправка сформированной строки в rabbit

    """
    print("Send to rabbit=", full_message)
    # credentials = pika.PlainCredentials(rabbit_login, rabbit_passw)
    # parameters = pika.ConnectionParameters('127.0.0.1', 5672, '/', credentials)
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()
    # channel.queue_declare(queue='in')
    # channel.basic_publish(exchange='',
    #                       routing_key='in',
    #                       body=full_message)
    # connection.close()


def split_data(hex_string, deveui):
    """
    разделяем строку на номер вирт устройства и нагрузку
    возвращаем в виде списка [deveui_new,payload,deveui_new,payload....]

    """
    cnt = 0
    answer = []
    # print(hex_string, type(hex_string))
    while cnt < len(hex_string):
        dev_num = hex_string[cnt]
        ch_num = hex_string[cnt + 1]
        ch_type = hex_string[cnt + 2:cnt + 4]
        if ch_type == '82':
            data_len = 8
        elif ch_type == '01':  # need more elif
            data_len = 2
        else:
            data_len = 4
        hex_payload = hex_string[cnt + 4:cnt + 4 + data_len]
        cnt = cnt + 4 + data_len
        answer.append(deveui + dev_num)
        answer.append('0' + ch_num + ch_type + hex_payload)
        if ch_type=='74':
            for i in range(1,10):
                answer.append('0' + hex (i) + ch_type + hex_payload)
    # print('ans', answer)
    return (answer)


def process_raw(msg):  # добавляем экранирование для двойных кавычек,заблочено костылем
    # tmp = []
    # for c in msg:
    #     if c == "'":
    #         tmp.append('\"')
    #     else:
    #         tmp.append(c)
    # # result=''.join(tmp)
    result = "dumb string"
    return result


##################### START HERE#########################################################

with open("C:\IOT\Python\settings.txt", "r") as read_file:
    data = json.load(read_file)
rabbit_adr = data.get("rabbit")  # вытаскиваем параметрв кролика
rabbit_login = data.get("rabbit_login")
rabbit_passw = data.get("rabbit_passw")
rabbit_port = data.get("rabbit_port")

gateway_list = data.get("GW_list")  # получаем список шлюзов
gateway_list = list(gateway_list.split(","))  # в список
print("Gateway list=", gateway_list)
gw_dict = {gateway_list[i]: time.time() for i in
           range(len(gateway_list))}  # делаем словарь с временем последнего коннекта
gw_dict_status = {gateway_list[i]: "WARNING" for i in
                  range(len(gateway_list))}  # словарь со статусом  шлюза  can be  OK, WARNING, FAULT
gateway_delay = data.get("GW_delay")  # установки для границ проверки
gateway_fault = data.get("GW_fault")
devEui_list = data.get("multiEmeter")  # получаем список multiEmeter
devEui_list = list(devEui_list.split(","))
print("Multi E-METER list=", devEui_list)
netserver_adr = data.get("netserver")  # вытаскиваем параметры веги
net_login = data.get("net_login")
net_passw = data.get("net_passw")
# print(netserver_adr)

ws = websocket.create_connection(netserver_adr)
print("Подключаемся...")
ws.settimeout(10)
cred = '{"cmd":"auth_req", "login":"' + net_login + '", "password":"' + net_passw + '"}'
# print (cred)
ws.send(cred)
ws_conn = ws.recv()
if ws_conn.find('token') == -1:
    print("не удалось подключиться, выход")
    input("нажмите Enter для выхода")
    exit(3)
else:
    print("Успешное подключение!")


while True:  # заменить на True- infinite loop
    try:
        json_string = ws.recv()  # забираем сообщение как оно будет готово
    except websocket._exceptions.WebSocketTimeoutException:
        json_string = '{"cmd":"ws_timeout"} '     #перехватываем исключение чтобы не попадать в блокировку сокетом
        print(json_string)
    if len(json_string) != 0:
            data = json.loads(json_string)
            devEui = data.get('devEui')
            mess = data.get("message")
            ###  блок парсинга мультсчетчиков
            if devEui in devEui_list:
                if data.get('type') == 'CONF_UP' and data.get('port')!=200:
                    devEui = str(data.get('devEui'))
                    data_list = split_data(data.get('data'), devEui)
                    num_of_send = int(len(data_list))
                    # print(num_of_send)
                    dt = datetime.datetime.now().isoformat() + '+05:00'
                    raw_message = process_raw(json_string)

                    for i in range(0, num_of_send - 1, 2):
                        list_mess = {"hex": data_list[i + 1], "ServerId": 2, "port": (data.get('port')),
                                     "rssi": (data.get('rssi')),
                                     "snr": (data.get('snr')), "date": dt, "rawMessage": raw_message, "direction": 0,
                                     "devEui": data_list[i]}
                        str_mess = str(list_mess)
                        a = '"'
                        b = "'"
                        print ('send to rabbit multi emeter ')
                        #print(devEui, data_list[i + 1])
                        throw_to_rabbit(str_mess.replace(b, a))
            ##блок проверки таймаутов шлюза
            if mess != None:
                gw = ""
                if "LATENCY" in mess:
                    gw = mess[3:19]
                if "GW-" in mess:
                    gw = mess[6:22]
                if gw != "":
                    gw_dict[gw] = time.time()
                    if gw_dict_status[gw] != "OK":
                        gw_dict_status[gw] = "OK"
                        #print("OK>>", gw, time.ctime(time.time()))
                        # trow to rabbit OK
                        ss = '{"hex": "00", "ServerId": 2, "port": 55, "rssi": 0,"snr": 0, "date":"' + datetime.datetime.now().isoformat() + '+05:00' + '","rawMessage": "GW OK", "direction": 0, "devEui": "' + gw + '"}'
                        #print("GW_OK message=", ss)            # throw_to_rabbit(ss)
                        throw_to_rabbit(ss)
            curr_time = time.time()
            for items in gateway_list:
                delta = int(curr_time - gw_dict[items])
                if delta > gateway_delay and delta < gateway_fault:
                    #print("WARNING>>", items, delta, time.ctime(curr_time))
                    if gw_dict_status[items] != "WARNING":
                        gw_dict_status[items] = "WARNING"
                        # throw to rabbit WARN
                        ss = '{"hex": "01", "ServerId": 2, "port": 55, "rssi": 0,"snr": 0, "date":"' + datetime.datetime.now().isoformat() + '+05:00' + '","rawMessage": "GW WARNING", "direction": 0, "devEui": "' + items + '"}'
                        #print("GW_WARNING message=", ss)          # throw_to_rabbit(ss)
                        throw_to_rabbit(ss)
                if delta > gateway_fault:
                    # print("ALARM>>", items, int(delta), time.ctime(curr_time))
                    gw_dict_status[items] = "FAULT"
                    ss = '{"hex": "02", "ServerId": 2, "port": 55, "rssi": 0,"snr": 0, "date":"' + datetime.datetime.now().isoformat() + '+05:00' + '","rawMessage": "GW FAULT", "direction": 0, "devEui": "' + items + '"}'
                    #print("GW_FAULT message=", ss)      # throw_to_rabbit(ss)
                    throw_to_rabbit(ss)