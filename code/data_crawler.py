import re
from bs4 import BeautifulSoup
import os
import json
import csv
from datetime import datetime

# Find the information satisfying the params
# The params are actually used for finding the tag PREVIOUS to the tag containing the data
# For example, the td tag "địa chỉ giao hàng" will appear before the span tag that contains the needed data
def find_info(soup=None,tag=None,text_value=None,attr_value=None):
    if text_value is not None:
        input_text = re.compile(text_value)
    else:
        input_text = None
    span_tag = soup.find(tag, text=input_text, attrs=attr_value)
    if span_tag is not None:
        next_span = span_tag.find_next("span")
        if next_span is not None:
            text = next_span.text
            return text.strip()  
    return None

# Extract promotions from the orders
def find_promo(soup, order_id):
    all_promo_tr = soup.find_all('tr',style="font-family:Helvetica, 'Arial', sans-serif;-webkit-text-size-adjust:none;font-weight:normal;color:#000000;")
    promo_list = []
    for tr in all_promo_tr:
        span_tag = tr.find_all('span')
        promo_code = span_tag[0].text.strip()
        value = format_number(span_tag[1].text.strip())
        promo = {'code':promo_code,'value':value,'order_id':order_id}
        promo_list.append(promo)
    return promo_list

# Extract items from the orders
def find_item(soup, order_id):
    tr_all = soup.find_all('tr',style='color:#000000;')
    item_list = []
    for tr in tr_all:
        span_tag = tr.find_all('span')
        quantity = format_number(span_tag[0].text.strip())
        if quantity == None:
            prev_item = item_list[-1]
            note = span_tag[1].text.strip()
            if prev_item['note'] != "":
                prev_item['note'] += ', ' + note
            else:
                prev_item['note'] = note
        else:
            product = span_tag[1].text.strip()
            price = format_number(span_tag[2].text.strip())
            item = {'quantity':quantity, 'product_name':product, 'price':price, 'order_id':order_id, 'note':""}
            item_list.append(item)

    # for item in item_list:
    #     if item['note'] == '':
    #         item['note'] = None
    return item_list

# Extract information of the order (not include user personal information)
def find_order(soup, order_id):
    item_list = find_item(soup,order_id)
    
    raw_cost = format_number(find_info(soup,tag='span',text_value='Tổng tạm tính'))

    shipping_cost = format_number(find_info(soup,tag='span',text_value='Cước phí giao hàng'))

    service_cost = format_number(find_info(soup,tag='span',text_value='Phí dịch vụ'))

    user_paid = format_number(find_info(soup,tag='span',text_value='BẠN TRẢ'))

    promo_list = find_promo(soup,order_id)

    return item_list, raw_cost, shipping_cost, service_cost, user_paid, promo_list

# Extract information in a receipt 
def get_receipt(email_path):
    with open(email_path, "r", encoding="utf-8") as html_file:
        html_content = html_file.read()

    soup = BeautifulSoup(html_content, "html.parser")
    soup.prettify(formatter=lambda s: s.replace(u'\xa0', ''))

    user = find_info(soup,tag='span',text_value='Người dùng')
    order_id = find_info(soup,tag='span',text_value='Mã đặt xe')
    store = find_info(soup,tag='span',text_value='Điểm đón khách')
    destination = format_address(find_info(soup,tag='span',text_value='Điểm trả khách'))
    time = format_time(find_info(soup,tag='td',attr_value={'class': 'produceTdLast', 'style': 'font-size: 12px; line-height: 21px; font-weight: bold;', 'width': '56%'}))
    payment_method = find_info(soup,tag='td',attr_value={'style':'font-size: 11px; line-height: 18px; font-family: \'\',Helvetica;'})    
    item_list,raw_cost,shipping_cost,service_cost,user_paid,promo_list = find_order(soup,order_id)

    receipt = {'user' : user, 'order_id' : order_id, 'store' : store, 'destination' : destination, 'time' : time, 'payment_method' : payment_method, 
               'raw_cost' : raw_cost, 'shipping_cost' : shipping_cost, 'service_cost' : service_cost, 'user_paid' : user_paid}
    return receipt, item_list, promo_list

# Convert list of receipts, list of products, list of promotions to csv files
def convert_to_csv(dict_list,folder_path,file_name):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    filepath = os.path.join(folder_path,file_name)
    fieldnames = dict_list[0].keys()

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for item in dict_list:
            writer.writerow(item)

# Extract number only
def format_number(input):
    if input is None or input == '':
        return None
    return re.findall(r'\d+',input)[0]

# Convert the time to date/month/year hour/minute GMT format
def format_time(input):
    if input is None or input == '':
        return None
    parsed_time = datetime.strptime(input, "%d %b %y %H:%M %z")
    formatted_time = parsed_time.strftime("%Y/%m/%d %H:%M:%S")
    return formatted_time

# Grab use aliases for some locations corresponding to user's setting; therefore, we have to set it back to normal addresses
def format_address(input):
    if input == 'Nhà':
        return '1/12 Công Trường Tự Do, P.19, Q.Bình Thạnh, Hồ Chí Minh, 70000, Vietnam'
    if input == 'Trường':
        return '268 Lý Thường Kiệt, P.14, Q.10, Hồ Chí Minh, 70000, Vietnam'
    return input

if __name__ == '__main__':
    folder_path = "../data/email"
    data_path = "../data/data"
    files = sorted(os.listdir(folder_path))
    all_receipt = []
    all_item = []
    all_promo = []

    # Extract informations from the receipts
    for i,file in enumerate(files,1):
        email_path = os.path.join(folder_path,file)
        receipt, item_list, promo_list = get_receipt(email_path)

        if receipt['order_id'] != None:
            all_receipt.append(receipt)
            all_item.append(item_list)
            all_promo.append(promo_list)
    all_item = [item for sublist in all_item for item in sublist]
    all_promo = [promo for sublist in all_promo for promo in sublist]

    # Write the information to csv files
    convert_to_csv(all_receipt,data_path,'all_receipt.csv')
    convert_to_csv(all_item,data_path,'all_item.csv')
    convert_to_csv(all_promo,data_path,'all_promo.csv')


