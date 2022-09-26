# with final correction
from cgitb import small
from lib2to3.refactor import get_all_fix_names
import sqlite3
# imported sqlite 3
import csv
#  imported csv file module
conn = sqlite3.connect('record.db')
# here we connected database if it already exists it will be executed else
# a new database will be created
c = conn.cursor()
# communicating with database
# list stores  medium high low
dict = {}
#  creating a dictionary with name dict that stores industry name as key with values as change in price for loop
# was used for traversal
price = {}
companynames = []
dict_industry = {
    "Auto Ancillaries": [0] * 3,
    "Finance - General": [0] * 3,
    "Ceramics & Granite": [0] * 3}
# dict_industry was declared as declaring industry name and each industry as key and list that
# contains number of high and lows and medium in the list
c.execute("""CREATE TABLE Ticker(
        Date TEXT,
        Company_Name TEXT,
        Industry TEXT,
        Previous_Day_Price TEXT,
        Current_Price TEXT,
        Change_in_Price TEXT,
        Confidence TEXT
)""")
# ticker table was created using CREATE TABLE key word
c.execute("""CREATE TABLE Metrics(
        KPIs TEXT,
        Mertics TEXT
)""")
# Metrics table was created using CREATE TABLE key word
temp_string = '&%<=> '
# reading from control_table csv file
with open('Control/control-table.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for row in csv_reader:
        for char in temp_string:
            row[1] = row[1].replace(char, "").strip()

        if row[0] in dict.keys():
            if row[1] != "Previousdaynotlisted":
                dict[row[0]].append(row[1])
        else:
            if row[1] != "Previousdaynotlisted":
                dict[row[0]] = [row[1]]
# print(dict_industry)
# >>>>>>>>>>>>>>>>>>>>>>>...................................<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# following dictionary was created
# if the next is not the previous day not listed then row[1] is inserted in list corresponding to key
# print(dict)
# {'Finance - General': ['0', '04', '4'], 'Auto Ancillaries': ['0', '010', '10'], 'Ceramics & Granite': ['1', '12', '2']}
# dict_industry basicallty tells number of high lows for each industry
# here dict_industry gives
# {'Auto Ancillaries': [0, 0, 0], 'Finance - General': [0, 0, 0], 'Ceramics & Granite': [0, 0, 0]}
# >>>>>>>>>>>>>>>>>>>>>>>...................................<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
with open('Record/2021101075-20-5-2022.csv', 'r') as csv_file1:
    csv_reader2 = csv.reader(csv_file1)
    next(csv_reader2)

    for row1 in csv_reader2:
        price[row1[0]] = [float(row1[2])]
    #    companynames.append(row1[0])
    # here a comapany names list was used that contains name of comapny
        c.execute("INSERT INTO Ticker VALUES(?,?,?,?,?,?,?)",
                  (
                      '20-5-22',
                      #     date
                      row1[0],
                      #     company name
                      row1[1],
                      #     industry name
                      'N.A',
                      #     since its first day hence previous day price in NA
                      row1[2],
                      #     price
                      'N.A',
                      #     nor is change is price applicable
                      'Listed New'
                      #     no data hence confidence cant be calculated
                  )
                  )
# print(price)
# print(dict_industry)
# c.execute("SELECT * FROM Ticker ")
# Items=c.fetchall()
# for item in Items:
#       print(item)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# basically price is the dictionary which has key as company name stores price in the lisyt one after another
# by now the price ductionary contains
# {'Pricol': [104.9], 'Sharda Motor': [666.0], 'Auto Stampings': [448.55], 'Rajratan Global': [632.95], 'Lumax Auto Tech': [153.0],
#  'The Hi-Tech Gea': [192.95], 'Shivam Auto': [32.45], 'Autoline Ind': [60.55], 'HMT': [25.8], 'Precision Camsh': [110.1], 'Setco Auto'
# : [14.15], 'UCAL Fuel': [110.9], 'Eicher Motors': [2687.35], 'Tata Motors': [415.85]..etc etc by now only first day price
# dict_industry basicallty tells number of high lows for each industry
# here dict_industry gives
# {'Auto Ancillaries': [0,0,0], 'Finance - General': [0,0,0], 'Ceramics & Granite': [0,0,0]}
# ticker table here contains data like
# ('20-5-22', 'Pricol', 'Auto Ancillaries', 'N.A', '104.9', 'N.A', 'Listed New')
# ('20-5-22', 'Sharda Motor', 'Auto Ancillaries', 'N.A', '666', 'N.A', 'Listed New')
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
with open('Record/2021101075-21-5-22.csv', 'r') as csv_file2:
    csv_reader3 = csv.reader(csv_file2)
    next(csv_reader3)

    for row2 in csv_reader3:
        varans = ""
        confidence = ""
        prev_price = ""
        # if and else condition were introduced so as new company if added can
        # be taken into account
        if row2[0] in price.keys():
            prev_price = price[row2[0]][-1]
            price[row2[0]].append(float(row2[2]))
            varans = (
                ((float(row2[2]) - prev_price) / float(prev_price)) * 100)
            # var ans stores change in price corrrespoonding to each comapny
            # final rpice -initial price
            industry = row2[1]
            #    row2[1] stores the industry name as in record csv files
            if(float(varans) < float(dict[industry][0])):
                confidence = "LOW"
                dict_industry[industry][2] += 1
            elif(float(varans) > float(dict[industry][2])):
                confidence = "HIGH"
                dict_industry[industry][1] += 1
            # 1st meneber corresponding to key of each industry stores high
            # count and 2 stores low count
            else:
                confidence = "MEDIUM"
                dict_industry[industry][0] += 1
        else:
            price[row2[0]] = []
            price[row2[0]].append(float(row2[2]))
            varans = 'NA'
            confidence = 'Listed New'
            prev_price = 'NA'

        c.execute("INSERT INTO Ticker VALUES(?,?,?,?,?,?,?)",
                  (
                      '21-5-22',
                      #     date
                      row2[0],
                      #     company nae
                      row2[1],
                      #     industry name
                      prev_price,
                      #     previous day price
                      row2[2],
                      #    current
                      varans,
                      #    change in price
                      confidence
                  )
                  )

# print("dict_industry")
# print(dict_industry)
# print("price")
# print(price)
# c.execute("SELECT * FROM Ticker ")
# Items=c.fetchall()
# for item in Items:
#       print(item)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# data entered into ticker as date, company name as 0th coloumn, industry name as 1st coloumn,previous day price which in this case is at
# dict_industry basicallty tells number of high lows for each industry
# here dict_industry gives
# {'Auto Ancillaries': [6, 5, 8], 'Finance - General': [15, 14, 11], 'Ceramics & Granite': [2, 1, 27]}
# price[industryname][0]// then current price which is given as 2nd col, then chnage as calc by var ans
# here dict_industry gives
# price[industryname][0]// then current price which is given as 2nd col, then chnage as calc by var ans
# {'Auto Ancillaries': [6, 5, 8], 'Finance - General': [15, 14, 11], 'Ceramics & Granite': [2, 1, 27]}
# price dictionary by now contains following data
# {'Pricol': [104.9, 99.655], 'Sharda Motor': [666.0, 632.7], 'Auto Stampings': [448.55, 426.1225],
# 'Rajratan Global': [632.95, 601.3025], 'Lumax Auto Tech': [153.0, 145.35], 'The Hi-Tech Gea':
#  [192.95, 183.3025], 'Shivam Auto': [32.45, 30.8275], 'Autoline Ind': [60.55, 66.605],
# 'HMT': [25.8, 28.38], 'Precision Camsh': [110.1, 121.11], 'Setco Auto': [14.15, 15.565]...etc etc ire previous day prices
# ticker contains date data as
# ('21-5-22', 'Man Infra', 'Ceramics & Granite', '92.95', '83.655', '-10.0', 'LOW')
# ('21-5-22', 'Murudeshwar Cer', 'Ceramics & Granite', '23.7', '21.33', '-10.0', 'LOW')
# #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
with open('Record/2021101075-22-5-22.csv', 'r') as csv_file3:
    csv_reader4 = csv.reader(csv_file3)
    next(csv_reader4)

    for row3 in csv_reader4:
        varans = ""
        confidence = ""
        prev_price = ""
        # if and else condition were introduced so as new company if added can
        # be taken into account
        if row3[0] in price.keys():
            prev_price = price[row3[0]][-1]
            price[row3[0]].append(float(row3[2]))
            varans = (
                ((float(row3[2]) - prev_price) / float(prev_price)) * 100)
            # var ans stores change in price corrrespoonding to each comapny
            # final rpice -initial price
            industry = row3[1]
            #    row2[1] stores the industry name as in record csv files
            if(float(varans) < float(dict[industry][0])):
                confidence = "LOW"
                dict_industry[industry][2] += 1
            elif(float(varans) > float(dict[industry][2])):
                confidence = "HIGH"
                dict_industry[industry][1] += 1
            # 1st meneber corresponding to key of each industry stores high
            # count and 2 stores low count
            else:
                confidence = "MEDIUM"
                dict_industry[industry][0] += 1
        else:
            price[row3[0]] = []
            price[row3[0]].append(float(row3[2]))
            varans = 'NA'
            confidence = 'Listed New'
            prev_price = 'NA'

        c.execute("INSERT INTO Ticker VALUES(?,?,?,?,?,?,?)",
                  (
                      '22-5-22',
                      #     date
                      row3[0],
                      #     company nae
                      row3[1],
                      #     industry name
                      prev_price,
                      #     previous day price
                      row3[2],
                      #    current
                      varans,
                      #    change in price
                      confidence
                  )
                  )
# print("dict_industry")
# print(dict_industry)
# print("price")
# print(price)
# c.execute("SELECT * FROM Ticker ")
# Items=c.fetchall()
# for item in Items:
#       print(item)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.
#  by now dict_industry contains following data like
# {'Auto Ancillaries': [8, 7, 23], 'Finance - General': [15, 36, 29],
# 'Ceramics & Granite': [2, 13, 45]}
# # price dict conatins value of the type like
# 'Pricol': [104.9, 99.655, 87.6964], 'Sharda Motor': [666.0, 632.7, 556.776],
#  'Auto Stampings': [448.55, 426.1225, 374.9878], 'Rajratan Global': [632.95, 601.3025, 529.1462],
#   'Lumax Auto Tech': [153.0, 145.35, 127.908], 'The Hi-Tech Gea': [192.95, 183.3025, 161.3062],
#    'Shivam Auto': [32.45, 30.8275, 27.1282],
# # table by now has following data like
# ('22-5-22', 'Madhav Marbles', 'Ceramics & Granite', '55.284', '48.64992', '-12.0', 'LOW')
# ('22-5-22', 'Ramky Infra', 'Ceramics & Granite', '175.287', '154.25256', '-12.0', 'LOW')
# ('22-5-22', 'Visaka Ind', 'Ceramics & Granite', '476.55', '419.364', '-12.0', 'LOW')
# >>>">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
with open('Record/2021101075-23-5-22.csv', 'r') as csv_file4:
    csv_reader5 = csv.reader(csv_file4)
    next(csv_reader5)

    for row4 in csv_reader5:
        varans = ""
        confidence = ""
        prev_price = ""
        # if and else condition were introduced so as new company if added can
        # be taken into account
        if row4[0] in price.keys():
            prev_price = price[row4[0]][-1]
            price[row4[0]].append(float(row4[2]))
            varans = (
                ((float(row4[2]) - prev_price) / float(prev_price)) * 100)
            # var ans stores change in price corrrespoonding to each comapny
            # final rpice -initial price
            industry = row4[1]
            #    row2[1] stores the industry name as in record csv files
            if(float(varans) < float(dict[industry][0])):
                confidence = "LOW"
                dict_industry[industry][2] += 1
            elif(float(varans) > float(dict[industry][2])):
                confidence = "HIGH"
                dict_industry[industry][1] += 1
            # 1st meneber corresponding to key of each industry stores high
            # count and 2 stores low count
            else:
                confidence = "MEDIUM"
                dict_industry[industry][0] += 1
        else:
            price[row4[0]] = []
            price[row4[0]].append(float(row4[2]))
            varans = 'NA'
            confidence = 'Listed New'
            prev_price = 'NA'

        c.execute("INSERT INTO Ticker VALUES(?,?,?,?,?,?,?)",
                  (
                      '23-5-22',
                      #     date
                      row4[0],
                      #     company nae
                      row4[1],
                      #     industry name
                      prev_price,
                      #     previous day price
                      row4[2],
                      #    current
                      varans,
                      #    change in price
                      confidence
                  )
                  )
# print("dict_industry")
# print(dict_industry)
# print("price")
# print(price)
# c.execute("SELECT * FROM Ticker ")
# Items=c.fetchall()
# for item in Items:
#       print(item)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.>>>>>>>>>>>
# dict_industry by now has following data
# {'Auto Ancillaries': [12, 19, 26], 'Finance - General': [16, 53, 51], 'Ceramics & Granite': [7, 17, 66]}
# # price dict by now has these much prices like
# {'Pricol': [104.9, 99.655, 87.6964, 95.589076], 'Sharda Motor': [666.0, 632.7, 556.776, 606.88584],
# 'Auto Stampings': [448.55, 426.1225, 374.9878, 408.736702], 'Rajratan Global': [632.95, 601.3025, 529.1462, 576.769358],
#  'Lumax Auto Tech': [153.0, 145.35, 127.908, 141.97788], 'The Hi-Tech Gea': [192.95, 183.3025, 161.3062, 179.049882],
# #  ticker table another date is added with data as
# ('23-5-22', 'Choice Internat', 'Finance - General', '361.746', '394.30314', '9.0', 'HIGH')
# ('23-5-22', 'Alankit', 'Finance - General', '13.76235', '15.0009615', '9.00000000000001', 'HIGH')
# ('23-5-22', 'Indian Bank', 'Finance - General', '164.88045', '179.7196905', '9.00000000000001', 'HIGH')
# ('23-5-22', 'Muthoot Cap', 'Finance - General', '266.62545', '290.6217405', '8.99999999999999', 'HIGH')
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
with open('Record/2021101075-24-5-22.csv', 'r') as csv_file5:
    csv_reader6 = csv.reader(csv_file5)
    next(csv_reader6)

    for row5 in csv_reader6:
        varans = ""
        confidence = ""
        prev_price = ""
        # if and else condition were introduced so as new company if added can
        # be taken into account
        if row5[0] in price.keys():
            prev_price = price[row5[0]][-1]
            price[row5[0]].append(float(row5[2]))

            varans = (
                ((float(row5[2]) - prev_price) / float(prev_price)) * 100)
            # var ans stores change in price corrrespoonding to each comapny
            # final rpice -initial price
            industry = row5[1]
            #    row2[1] stores the industry name as in record csv files
            if(float(varans) < float(dict[industry][0])):
                confidence = "LOW"
                dict_industry[industry][2] += 1
            elif(float(varans) > float(dict[industry][2])):
                confidence = "HIGH"
                dict_industry[industry][1] += 1
            # 1st meneber corresponding to key of each industry stores high
            # count and 2 stores low count
            else:
                confidence = "MEDIUM"
                dict_industry[industry][0] += 1
        else:
            price[row5[0]] = []
            price[row5[0]].append(float(row5[2]))
            varans = 'NA'
            confidence = 'Listed New'
            prev_price = 'NA'
        companynames.append(row5[0])
        c.execute("INSERT INTO Ticker VALUES(?,?,?,?,?,?,?)",
                  (
                      '24-5-22',
                      #     date
                      row5[0],
                      #     company nae
                      row5[1],
                      #     industry name
                      prev_price,
                      #     previous day price
                      row5[2],
                      #    current
                      varans,
                      #    change in price
                      confidence
                  )
                  )
# print("dict_industry")
# print(dict_industry)
# print("price")
# print(price)
# c.execute("SELECT * FROM Ticker ")
# Items=c.fetchall()
# for item in Items:
#       print(item)
##########################################################################
# by now we have dict_industry as
#   {'Auto Ancillaries': [29, 21, 26], 'Finance - General': [16, 78, 66], 'Ceramics & Granite': [7, 39, 74]}
#  we have price as
# 'Arman Financial': [996.0, 956.16, 841.4208, 765.692928, 834.6052915], 'Nagreeka Cap'
# : [11.1, 12.21, 10.7448, 9.777768, 10.65776712], 'Transwarranty': [9.2, 10.12, 8.9056, 8.104096, 8.83346464],
#  'Palash Securiti': [101.1, 111.21, 97.8648, 89.056968, 97.07209512],
#  'Industrial Inv': [118.45, 130.295, 114.6596, 104.340236, 113.7308572], 'Future Consumer':
#  [2.4, 2.64, 2.3232, 2.114112, 2.30438208],
#   ticker table rows added be like
# ('24-5-22', 'AGI Greenpac', 'Ceramics & Granite', '203.0453568', '203.0453568', '0.0', 'LOW')
# ('24-5-22', 'CCCL', 'Ceramics & Granite', '2.45509992', '2.45509992', '0.0', 'LOW')
# ('24-5-22', 'Setubandhan Inf', 'Ceramics & Granite', '2.55530808', '2.55530808', '0.0', 'LOW')
##########################################################################
# >>>>>>>>besrt listed and worst  listed>>>>>>>>>>>>>>>>>
# here i took three variable each one store number of highs encountered
# for that particular industry and gives maximum of three
auto = dict_industry["Auto Ancillaries"][1]
# auto stores number of high in auto ancilliaries
finance = dict_industry["Finance - General"][1]
#  finance stores numbers of high of finance-general
ceramics = dict_industry["Ceramics & Granite"][1]
#  cermaics stores high of cermaics
if (auto >= finance) and (auto >= ceramics):
    largest = "Auto Ancillaries"

elif (finance >= auto) and (finance >= ceramics):
    largest = "Finance - General"
else:
    largest = "Ceramics & Granite"
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Best listed Industry",
              largest
          )
          )
gain = {}
# gain is dict which store comapny name as key value and gains percentage
# corresponding to it
gain_num = {}
# basically gain is and dictionary and key corresponds to company name and
# stores gain percentage
for each in companynames:
    gain[each] = (price[each][-1] - price[each][0]) / (price[each][0]) * 100
    gain_num[each] = (price[each][-1] - price[each][0])
maximum = -90

for eachc in companynames:
    if(gain[eachc] > maximum):
        maximum = gain[eachc]
        best_company = eachc
    elif(gain[eachc] == maximum):
        if(gain_num[best_company] < gain_num[eachc]):
            best_company = eachc
        elif(gain_num[best_company] == gain_num[eachc]):
            if(best_company > eachc):
                best_company = eachc
# basically comapny having maximum gain percentage is the best one so once
# we found we insert it
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Best Company",
              best_company
          )
          )
# then we insert gain percentage
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Gain %",
              maximum
          )
          )
# similarly to gain percentage we do for loss percentage
loss = {}
# loss stores key as comapny name and values as loss percentage
loss_num = {}
maximum1 = -90
for each in companynames:
    loss[each] = -(price[each][-1] - price[each][0]) / (price[each][0]) * 100
    loss_num[each] = -(price[each][-1] - price[each][0])
for eachc2 in companynames:
    if(loss[eachc2] > maximum1):
        maximum1 = loss[eachc2]
        worst_company = eachc2
    elif(loss[eachc2] == maximum):
        if(loss_num[worst_company] < loss_num[eachc2]):
            worst_company = eachc2
        elif(loss_num[worst_company] == loss_num[eachc2]):
            if(worst_company > eachc2):
                worst_company = eachc2
# print(worst_company)
# print(maximum1)
auto1 = dict_industry["Auto Ancillaries"][2]
finance1 = dict_industry["Finance - General"][2]
ceramics1 = dict_industry["Ceramics & Granite"][2]
if (auto1 >= finance1) and (auto1 >= ceramics1):
    smallest = "Auto Ancillaries"
elif (finance1 >= auto1) and (finance1 >= ceramics1):
    smallest = "Finance - General"
else:
    smallest = "Ceramics & Granite"
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Worst listed Industry",
              smallest
          )
          )
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Worst Company",
              worst_company
          )
          )
c.execute("INSERT INTO Metrics VALUES(?,?)",
          (
              "Loss %",
              -maximum1
          )
          )


# print(largest)
# print(smallest)
# print(dict_industry)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>gain percentage>>>>>
# for each company you calculate the the last day price minus first day
# price so you get gain percentage for each day


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>loss percentage>>>>>

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# print(dict_industry)
# print(companynames)
# autopep8 --in-place --aggressive --aggressive Record-Service.py
# c.execute("SELECT * FROM Metrics")
# Items = c.fetchall()
# for item in Items:
#     print(item)
# print(companynames)
conn.commit()
c.close()
