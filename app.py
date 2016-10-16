import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application, ServiceBase, Unicode


from spyne import Iterable

from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.decorator import srpc

from spyne.server.wsgi import WsgiApplication
import requests, re

class CrimeStatistics(ServiceBase):
    @srpc(Unicode, Unicode, Unicode, _returns=Iterable(Unicode))
    def checkcrime(lat, lon, radius):

        url = "https://api.spotcrime.com/crimes.json?lat="+lat+"&lon="+lon+"&radius="+radius+"&key=."
        data = requests.get(url)
        json_obj = data.json()

        street_list = []
        type_list = []
        time12_3_am = 0
        time12_3_pm = 0
        time3_6_am = 0
        time3_6_pm = 0
        time6_9_am = 0
        time6_9_pm = 0
        time9pm_12midnight = 0
        time9am_12noon = 0

        for key in json_obj['crimes']:

            address = re.split(r'\bBLOCK', key["address"])

            if re.search("BL", address[0]) or re.search(r'\bST', address[0]) or re.search(r'\bAV',
                                                                                          address[0]) or re.search(
                    r'\bBROADWAY', address[0]) or re.search(r'\bBOULEVARD', address[0]) or re.search(r'\bRD',
                                                                                                     address[0]):
                temp1 = address[0].strip().replace("OF ", "")
                tmp1 = re.split("&|AND|/", temp1)   
                street_list.append(tmp1[0].strip())
                if (len(tmp1) > 1):
                    street_list.append(tmp1[1].strip())
            if (len(address) > 1) and (
                                        re.search("BL", address[1]) or re.search(r'\bST', address[1]) or re.search(
                                    r'\bAV', address[1]) or re.search(r'\bBROADWAY', address[1]) or re.search(
                            r'\bBOULEVARD', address[1]) or re.search(r'\bRD', address[1])):
                temp1 = address[1].strip().replace("OF ", "")
                tmp2 = temp1.split("&")
                street_list.append(tmp2[0].strip())
                if (len(tmp2) > 1):
                    street_list.append(tmp2[1].strip())
            if (len(address) > 2) and (
                                        re.search("BL", address[2]) or re.search(r'\bST', address[2]) or re.search(
                                    r'\bAV', address[2]) or re.search(r'\bBROADWAY', address[2]) or re.search(
                            r'\bBOULEVARD', address[2]) or re.search(r'\bRD', address[2])):
                temp1 = address[2].strip().replace("OF ", "")
                tmp3 = temp1.split('[&|AND]')
                street_list.append(tmp3[0].strip())
                if (len(tmp3) > 1):
                    street_list.append(tmp3[1].strip())

            type_list.append(key["type"])
            date = re.split('\s+', key["date"])

            time = re.split(':', date[1])

            if (int(time[0]) >= 1 and int(time[0]) <= 2) or (int(time[0]) == 12 and int(time[1]) > 0) or (
                    int(time[0]) == 3 and int(time[1]) == 0):
                if date[2] == "AM":
                    time12_3_am += 1
                if date[2] == "PM":
                    time12_3_pm += 1
            if (int(time[0]) >= 4 and int(time[0]) <= 5) or (int(time[0]) == 3 and int(time[1]) > 0) or (
                    int(time[0]) == 6 and int(time[1]) == 0):
                if date[2] == "AM":
                    time3_6_am += 1
                if date[2] == "PM":
                    time3_6_pm += 1
            if (int(time[0]) >= 7 and int(time[0]) <= 8) or (int(time[0]) == 6 and int(time[1]) > 0) or (
                    int(time[0]) == 9 and int(time[1]) == 0):
                if date[2] == "AM":
                    time6_9_am += 1
                if date[2] == "PM":
                    time6_9_pm += 1
            if (int(time[0]) >= 10 and int(time[0]) <= 11) or (int(time[0]) == 9 and int(time[1]) > 0) or (
                    int(time[0]) == 12 and int(time[1]) == 0):
                if (date[2] == "AM" and int(time[0]) == 12) or (date[2] == "PM" and int(time[0]) != 12):
                    time9pm_12midnight += 1
                if (date[2] == "PM" and int(time[0]) == 12) or (date[2] == "AM" and int(time[0]) != 12):
                    time9am_12noon += 1

        street_counter = {}
        for street in street_list:
            if street in street_counter:
                street_counter[street] += 1
            else:
                street_counter[street] = 1

        popular_street = sorted(street_counter, key=street_counter.get, reverse=True)

        top_3 = popular_street[:3]

        type_counter = {}
        total_crimes = 0
        for type in type_list:
            total_crimes += 1
            if type in type_counter:
                type_counter[type] += 1
            else:
                type_counter[type] = 1


        yield ({'total_crimes': total_crimes, 'the_most_dangerous_streets': top_3 , 'crime_type_count': type_counter, 'event_time_count' : { '12:01am-3am' : time12_3_am, '3:01am-6am' : time3_6_am, '6:01am-9am' : time6_9_am, '9:01am-12noon' : time9am_12noon, '12:01pm-3pm' : time12_3_pm, '3:01pm-6pm' : time3_6_pm, '6:01pm-9pm' : time6_9_pm, '9:01pm-12midnight' : time9pm_12midnight }})

application = Application([CrimeStatistics],
    tns='spyne.crime',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()