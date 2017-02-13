import re

import requests

import storer

class maiair_scaper(object):

    def store_into_mysql(self, data):
        db_id = 'root'
        db_password = "1234"
        mysql_storer = storer.mysql_storer(db_id, db_password)

        db_name = 'MAIAIR'
        mysql_storer.create_database(db_name)

        mysql_storer.drop_table('flight_schedule')

        createTable_query = (
            "CREATE TABLE `flight_schedule` ("
            "  `product_no` int(11) NOT NULL AUTO_INCREMENT,"
            "  `flight_number` varchar(10) NOt NULL,"
            "  `origin` varchar(10) NOt NULL,"
            "  `destination` varchar(10) NOt NULL,"
            "  `depart_time` varchar(10) NOt NULL,"
            "  `arrive_time` varchar(10) NOt NULL,"
            "  `effective` varchar(20) NOt NULL,"
            "  `weekday` varchar(10) NOt NULL,"
            "  PRIMARY KEY (`product_no`)"
            ") ENGINE=InnoDB")
        mysql_storer.create_table(createTable_query)

        insert_query = ("INSERT INTO flight_schedule "
                       "(flight_number, origin, destination, depart_time, arrive_time, effective, weekday) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")

        for row in data:
            mysql_storer.insert_data(insert_query, row)

        mysql_storer.close()

    def flightSchedule_scraping(self):
        column = ['fligh_number', 'date', 'from', 'to', 'depart_time', 'arrive_time', 'via', 'partner', 'created_at']

        url = "http://maiair.com/MyanmarService.asmx"
        headers = {'content-type': 'text/xml'}
        body = """<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><FlightSchedule xmlns="http://tempuri.org/"><city></city></FlightSchedule></soap:Body></soap:Envelope>"""
        r = requests.post(url, data=body, headers=headers)

        row_indexes = [m.start() for m in re.finditer("rowscss", r.content)]

        flights = []
        for counter in range(0, len(row_indexes)):
            flight = []
            try:
                row_text = r.content[row_indexes[counter]:row_indexes[counter+1]]
            except Exception:
                row_text = r.content[row_indexes[counter]:]

            item_indexes = [m.start() for m in re.finditer("/td", row_text)]
            for k in range(0, 6):
                check_index = item_indexes[k]
                while row_text[check_index-3:check_index] != "gt;":
                    check_index = check_index - 1

                flight.append(row_text[check_index:item_indexes[k]].replace("&lt;", ""))

            week = ['Mon', 'Tue', 'Wen', 'Thur', 'Fri', 'Sat', 'Sun']
            weekday = ""
            for k in range(5, len(item_indexes)-1):
                if "icon.png" in row_text[item_indexes[k]: item_indexes[k+1]]:
                    weekday = weekday + week[k-5] + " | "

            flight.append(weekday[:-3])
            flights.append(flight)

        return flights

if __name__ == "__main__":
    scraper = maiair_scaper()
    data = scraper.flightSchedule_scraping()
    scraper.store_into_mysql(data)
