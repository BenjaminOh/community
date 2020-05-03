# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime


# conn_string = "host='127.0.0.1' dbname='postgres' user='homestead' password='secret' port='54320'"
conn_string = "host='45.63.38.233' dbname='tm' user='tm' password='Akdntm7682' port='5432'"
conn = psycopg2.connect(conn_string)
cur = conn.cursor()



def setDate(reg_dt):
    tmpRegDt = reg_dt.split()[1]
    nowDate = datetime.today().strftime("%Y-%m-%d")
    # print (datetime.today().strftime("%Y-%m-%d"))
    if len(tmpRegDt) == 5:
        tmpRegDt = nowDate + ' ' + tmpRegDt
    tmpRegDt = tmpRegDt.replace(".", "-")
    return tmpRegDt;


def crawDbProc(site_code, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, like_cnt, pic_flag, recommand_cnt, ref_url):
    print('crawDbProc => ', site_code, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, like_cnt, pic_flag, recommand_cnt, ref_url)
    # print('category_nm', type(category_nm), category_nm)
    # print('subject', type(subject), subject)
    # print('member_nm', type(member_nm), member_nm)
    # print('member_srl', type(member_srl), member_srl)
    # print('reg_dt', type(reg_dt), reg_dt)
    # print('hit_cnt', type(hit_cnt), hit_cnt)
    # print('like_cnt', type(like_cnt), like_cnt)
    # print('pic_flag', type(pic_flag), pic_flag)
    # print('recommand_cnt', type(recommand_cnt), like_cnt)
    # print('ref_url', type(ref_url), ref_url)

    subject = subject.replace("'", "''")
    # subject = subject.replace('"', '')
    member_nm = member_nm.replace("'", "''")


    update_sql = "update t_community set subject = '" + subject + "' where ref_url = '" + ref_url + "'  RETURNING *"
    insert_sql = "insert into t_community(seq, site_id, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, like_cnt, pic_flag, recommand_cnt, ref_url) select nextval('seq_t_community'), '" + site_code + "', '" + str(category_nm) + "', '" + subject + "', '"+member_nm+"', '"+member_srl+"', '"+reg_dt+"', "+hit_cnt+", "+like_cnt+", '"+ pic_flag +"', "+recommand_cnt+", '" + ref_url + "' WHERE NOT EXISTS (SELECT * FROM upsert)"
    cur.execute("WITH upsert AS (" + update_sql + ") " + insert_sql)
    conn.commit()

    sql = "insert into t_community_hit(hit_seq, hit_cnt, seq, log_hit_cnt, log_like_cnt, log_recommand_cnt) values(nextval('seq_hit_seq'), 0, currval('seq_t_community'), "+hit_cnt+", "+like_cnt+", "+recommand_cnt+")"
    cur.execute(sql)
    conn.commit()

    return '';

def crawRuliweb(site_code, board_seq, board_nm, front_url ):
    page_no = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

    for i in page_no:

        url = front_url +"&page="+ str(i)
        print('')
        print('crawRuliweb', i, url)
        print('')
        temp_result = requests.get(url)
        soup = BeautifulSoup(temp_result.text, "html.parser")
        # titles = soup.select(".board_main.theme_default tr:not(.list_inner, .best, .notice) td.subject a.subject_link")
        titles = soup.select(".board_main.theme_default tr:not(.list_inner, .best, .notice)")
        # print('titles', titles)
        like_cnt = "0";
        for j in range(len(titles)):
            item = titles[j]
            # print(item)
            category_nm = item.find_all("a")[0].text.strip()
            subject = item.find_all("a")[1].text.strip()
            ref_url = item.find_all("a")[1].attrs['href'].strip()
            reply_cnt = item.find_all("span", {'class':'num'})
            member_nm = item.find_all("span", {'class':'writer'})[0].text.strip()
            recommand_cnt = item.find_all("strong")
            hit_cnt = item.find_all("strong")[1].text.strip()
            reg_dt = item.find_all("span", {'class':'time'})[0].text.strip()
            reg_dt = setDate(reg_dt)
            print(j, category_nm, subject, ref_url, reply_cnt, member_nm, recommand_cnt, hit_cnt, reg_dt)

            if len(reply_cnt) == 1:
                reply_cnt = reply_cnt[0].text.strip()
            else:
                reply_cnt = "0"

            if len(recommand_cnt) == 1:
                recommand_cnt = recommand_cnt[0].text.strip()
            else:
                recommand_cnt = "0"

            # update_sql = "update t_community set subject = '"+ article_title +"' where ref_url = '"+ article_href +"'  RETURNING *"
            # insert_sql = "insert into t_community(seq, site_id, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, recomment_cnt, ref_url) select nextval('seq_t_community'), '"+site_nm+"', '"+board_seq+"', '"+ article_title +"', '', '', '', 0, 0, '"+ article_href +"' WHERE NOT EXISTS (SELECT * FROM upsert)"
            result = crawDbProc(site_code, category_nm, subject, member_nm, '', reg_dt, hit_cnt, like_cnt, 'N', recommand_cnt, ref_url)
            # result = crawDbProc(site_code, category_nm, subject, member_nm, '', reg_dt, hit_cnt, like_cnt, pic_flag, recommand_cnt, ref_url)
            # site_code, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, recomment_cnt, ref_url

            # cur.execute("WITH upsert AS ("+ update_sql +") "+ insert_sql)
            # conn.commit()
            # print(titles[j].text.strip())

    return '';


def crawClien(site_code, board_seq, board_nm, front_url):
    page_no = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    for i in page_no:

        url = front_url +"&page="+ str(i)
        print('')
        print('crawClien', i, url)
        print('')
        temp_result = requests.get(url)
        soup = BeautifulSoup(temp_result.text, "html.parser")
        contents = soup.select(".list_item.symph_row ")

        for j in range(len(contents)):
            item = contents[j]
            tmpstr = item.find_all('div', attrs={'data-role':True})
            like_cnt = "0"

            for rolename in tmpstr:
                if rolename.attrs.get('data-role') == 'list-like-count':
                    like_cnt = rolename.find_all('span')[0].text.strip()

            subject = item.find_all("a", {'class':'list_subject'})[0].text.strip()
            category_nm = item.find_all("span", {'class':'shortname'})
            recommand_cnt = item.find_all("div", {'class':'list_symph'})
            ref_url = item.find_all("a")[0].attrs['href'].strip()
            reply_cnt = item.find_all("span", {'class':'rSymph05'})
            pic_flag = item.find_all("span", {'class':'icon_pic'})
            member_nm = item.find_all("span", {'class':'nickname'})[0].text.strip()
            member_img = item.find_all("span", {'class':'nickname'})
            hit_cnt = item.find_all("span", {'class':'hit'})[0].text.strip()
            reg_dt = item.find_all("span", {'class':'timestamp'})[0].text.strip()

            if len(recommand_cnt) == 1:
                recommand_cnt = recommand_cnt[0].find('span').text.strip()
            else:
                recommand_cnt = "0"

            ref_url = "https://www.clien.net" + ref_url

            # 카테고리 명 추
            if len(category_nm) == 1:
                category_nm = category_nm[0].text.strip()
            else:
                category_nm = ''

            # print('hit_cnt', hit_cnt, type(hit_cnt), len(hit_cnt))

            if hit_cnt != "":
                hit_cnt = hit_cnt
            else:
                hit_cnt = "0"

            # print('hit_cnt', hit_cnt, type(hit_cnt) )

            # 댓글이 있을 경우 처리
            if len(reply_cnt) == 1:
                reply_cnt = reply_cnt[0].text.strip()
            else:
                reply_cnt = "0"

            # 내용에 이미지가 있는지
            if len(pic_flag) == 1:
                pic_flag = "Y"
            else:
                pic_flag = "N"

            # 닉네임 처리
            if member_nm == "":
                img = member_img[0].find("img", alt=True)
                member_nm = img['alt']

            # update_sql = "update t_community set subject = '"+ article_title +"' where ref_url = '"+ article_href +"'  RETURNING *"
            # insert_sql = "insert into t_community(seq, site_id, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, recomment_cnt, ref_url) select nextval('seq_t_community'), '"+site_nm+"', '"+board_seq+"', '"+ article_title +"', '', '', '', 0, 0, '"+ article_href +"' WHERE NOT EXISTS (SELECT * FROM upsert)"
            result = crawDbProc(site_code, category_nm, subject, member_nm, '', reg_dt, hit_cnt, like_cnt, pic_flag, recommand_cnt, ref_url)
            # site_code, category_nm, subject, member_nm, member_srl, reg_dt, hit_cnt, recomment_cnt, ref_url

            # cur.execute("WITH upsert AS ("+ update_sql +") "+ insert_sql)
            # conn.commit()
            # print(titles[j].text.strip())

    return '';

site_info = {

    'clien': {
        'board_seq': ['kin', 'park'],
        'nm': ['아무거나질문', '모두의공원'],
        'url': ['https://www.clien.net/service/board/kin',
                'https://www.clien.net/service/board/park'
                ]
    },
    'rulliweb': {
        'board_seq': [300148, 300143],
        'nm': ['북유게', '유게'],
        'url': ['https://m.ruliweb.com/community/board/300148?view_best=1',
                'https://m.ruliweb.com/community/board/300143?view_best=1']
    }
}

def main():
    result = ''
    for nm in site_info.keys():
        site_code = nm
        board_info = site_info[nm]
        print(board_info, type(board_info))

        board_seqs = board_info['board_seq']
        board_nms = board_info['nm']
        baord_urls = board_info['url']
        site_board_size = len(board_seqs)
        print('site_board_size',site_board_size)

        for i in range(site_board_size):
            board_seq = board_seqs[i]
            board_nm = board_nms[i]
            board_url = baord_urls[i]
            print(site_code, board_seq, board_nm, board_url)
            if site_code == 'rulliweb':
                result = crawRuliweb(site_code, board_seq, board_nm, board_url)
                print(result)
            elif site_code == 'clien':
                result = crawClien(site_code, board_seq, board_nm, board_url)

    # result = crawling(code, page_count)
    # print(result)

main()

