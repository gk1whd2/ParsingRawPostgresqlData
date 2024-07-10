import struct
import os
import sys
from datetime import datetime, timezone, timedelta
sys.path.append('/home/johnny/JohnnyProjects/Common')
from dbManager import DBManager
import pandas as pd

db = DBManager('wedding')


PAGE_SIZE = 8*1024

def get_header_data(data):
    BYTE_ORDER= 'little'
    pd_lsn = int.from_bytes(data[:8], byteorder=BYTE_ORDER)
    pd_checksum = int.from_bytes(data[8:10],byteorder=BYTE_ORDER)
    pd_flags = int.from_bytes(data[10:12], byteorder=BYTE_ORDER)
    pd_lower = int.from_bytes(data[12:14], byteorder=BYTE_ORDER)
    pd_upper = int.from_bytes(data[14:16], byteorder=BYTE_ORDER)
    pd_special = int.from_bytes(data[16:18], byteorder=BYTE_ORDER)
    pd_pagesize_version = int.from_bytes(data[18:20], byteorder=BYTE_ORDER)
    pd_prune_xid = int.from_bytes(data[20:24], byteorder=BYTE_ORDER)

    return {'pd_lsn':pd_lsn ,
            'pd_checksum':pd_checksum,
            'pd_flags': pd_flags,
            'pd_lower':pd_lower,
            'pd_upper':pd_upper,
            'pd_special':pd_special,
            'pd_pagesize_version':pd_pagesize_version,
            'pd_prune_xid':pd_prune_xid}

def get_item_pointer(data, header):
    item_end = header['pd_lower']
    item_start = 24
    item_cnt = len(data[item_start:item_end])//4

    item_ids = []
    for i in range(item_cnt):
        item_id_data = data[item_start + i*4 : item_start +(i+1)*4]
        id_data = int.from_bytes(item_id_data, byteorder = 'little')

        offset = int.from_bytes(item_id_data[:2], byteorder = 'little')
        length = int.from_bytes(item_id_data[2:], byteorder = 'little')

        offset = id_data & 0b111111111111111
        flags = (id_data>>15) & 0b11
        length = (id_data>>17) & 0b111111111111111


        item_ids.append((offset, length,flags))

    return item_ids

def get_heap_tuple_header(item_data):
    BYTE_ORDER= 'little'
    t_xmin = int.from_bytes(item_data[:4], byteorder=BYTE_ORDER)
    t_xmax = int.from_bytes(item_data[4:8],byteorder=BYTE_ORDER)
    t_cid = int.from_bytes(item_data[8:12], byteorder=BYTE_ORDER)
    t_xvac = int.from_bytes(item_data[12:16], byteorder=BYTE_ORDER)
    t_ctid = int.from_bytes(item_data[16:22], byteorder=BYTE_ORDER)
    #t_infomask2 = int.from_bytes(item_data[22:24], byteorder=BYTE_ORDER)
    #t_infomask = int.from_bytes(item_data[24:26], byteorder=BYTE_ORDER)
    t_hoff = int.from_bytes(item_data[22:23], byteorder=BYTE_ORDER)

    return {'t_xmin':t_xmin,
            't_xmax':t_xmax,
            't_cid': t_cid, 
            't_xvac':t_xvac,
            't_ctid':t_ctid,
            #'t_infomask2':t_infomask2,
            #'t_infomask':t_infomask,
            't_hoff':t_hoff}


def get_table_column_info(table_name):
    query = f"""
        SELECT
            a.attname AS column_name,
            t.typname AS data_type,
            a.attlen AS length,
            CASE
                WHEN a.attnotnull THEN 'NO'
                ELSE 'YES'
            END AS is_nullable
        FROM
            pg_class c
        JOIN
            pg_attribute a ON c.oid = a.attrelid
        JOIN
            pg_type t ON a.atttypid = t.oid
        WHERE
            c.relname = '{table_name}'
            AND a.attnum > 0
            AND NOT a.attisdropped;
        """

    return db.select(query)



def extract_data_from_item(item):
    global table_column_info
    header = get_heap_tuple_header(item)
    t_hoff = header['t_hoff']

    row_data = {}
    b_data = item[t_hoff:]

    offset =0
    for col in table_column_info:
        name = col[0]
        col_type = col[1]
        size = col[2]
        try:
            if size == -1: # 가변 길이
                flag = b_data[offset]&0x01
                if flag == 0:
                    length = (struct.unpack_from("<I", b_data,offset)[0] >> 2) - 4
                    offset += 4
                else:
                    length = (b_data[offset]>>1) - 1
                    offset +=1
                    
                value = b_data[offset:offset+length].decode()
                offset += length
            else:
                #remain_space_for_padding = 8 - (offset - (offset//8)*8)
                length = size
                remain_space_for_padding = size - (offset - (offset//size)*size)
                if remain_space_for_padding < size:
                    offset += remain_space_for_padding

                if size == 4:
                    value, offset = struct.unpack_from("I", b_data, offset)[0], offset+4
                elif size == 8:
                    value, offset = struct.unpack_from("<Q", b_data, offset)[0], offset+8
                    if col_type == 'timestamp':
                        value= datetime(2000, 1, 1,tzinfo=timezone.utc) + timedelta(microseconds = value)
        except Exception as e:
            print("Row Data: ", row_data)
            print("Column Info : ", name, col_type, size)
            print(f"binary : \n{b_data}\n")
            print(f"Failed Data: \n{b_data[offset:offset+length]}")
            raise e

        row_data[name] = value
    return row_data

def get_page_data(page_data):
    global table_column_info
    header = get_header_data(page_data)
    item_id_list = get_item_pointer(page_data, header)

    global page_index

    print(f"[{page_index}] Checksum : {header['pd_checksum']}\tLower : {header['pd_lower']}\tUpper : {header['pd_upper']}\tDataCounts : {len(item_id_list)}")
    item_data = []
    for idx, item_idx in enumerate(item_id_list):
        if item_idx[2] != 1:
            if item_idx[2] == 2:
                print(f"\t[Item {idx} : {item_idx}] Is Redirected")
            else:
                #print(f"\t[Item {idx} : {item_idx}] Is not valid")
                pass
            continue
        item_raw_data = page_data[item_idx[0]:item_idx[0] + item_idx[1]]
        try:
            extracted_item_data = extract_data_from_item(item_raw_data)
        except Exception as e:
            print(f"item_index[{idx}] : ", item_idx)
            raise e
        item_data.append(extracted_item_data)

    if len(item_data) > 0:
        min_index = min([d['index'] for d in item_data])
        max_index = max([d['index'] for d in item_data])
        print(f"\t\tIndex : {min_index} ~ {max_index}")
    else:
        print("\t\tEmpty Page")



    return item_data

import time

if __name__ == '__main__':
    start_time = time.time()
    table_name= 'photo_info'
    table_column_info = get_table_column_info(table_name)
    sample_page_number = 0
    file_path = './25137_new'
    with open(file_path, 'rb') as f:
        data = f.read()

    pages = int(len(data) / PAGE_SIZE)

    print("Data Length :", len(data))
    print("Exist Pages :", pages)

    print("-"*20, "Start", "-"*20)

    total_data = []
    for page_index in range(pages):
        if page_index != 8:
            #continue
            pass
        page_raw_data = data[page_index*PAGE_SIZE: (page_index+1) * PAGE_SIZE]
        try:
            page_data = get_page_data(page_raw_data)
        except Exception as e:
            print("page_index :",page_index)
            print(e)
        total_data.extend(page_data)


    pd = pd.DataFrame(total_data)
    pd.to_csv('result_data.csv')

    end_time = time.time()
    print("Elapsed Time: ", end_time - start_time)

