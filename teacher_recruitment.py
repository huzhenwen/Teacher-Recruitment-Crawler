import os
import datetime
import requests
from time import sleep
import random
from lxml import etree
import pandas as pd


def get_proxy():
    return requests.get("http://192.168.1.157:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://192.168.1.157:5010/delete/?proxy={}".format(proxy))


url_list = ["http://xtrsks.xtrs.xiangtan.gov.cn/cmsczportal/files/html/output/sydwks/column1.html",
            "http://rsj.zhuzhou.gov.cn//c935/index.html"]
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/78.0.3904.70 Safari/537.36 ",
    "Accept-Encoding": ""
}

abs_path = os.path.split(os.path.realpath(__file__))[0]


def process_urls(urls):
    today_date = datetime.datetime.today()
    # start_date = today_date - datetime.timedelta(days=60)
    news = ["##今天的招聘信息：", ]
    for url in urls:
        print(f"获取{url}内容")
        data = get_html(url)
        latest_news = data[data["news_date"] == today_date]
        wanted_news = latest_news[
            latest_news["news_title"].str.contains("招聘")
            & latest_news["news_title"].str.contains("教师")
            & ~latest_news["news_title"].str.contains("拟聘")]
        if wanted_news.shape[0] != 0:
            print("内容不为空，拼接发送内容")
            if "xiangtan" in url:
                news.append("###湘潭：")
            if "zhuzhou" in url:
                news.append("###株洲：")
            for i, r in wanted_news.iterrows():
                if "xiangtan" in url:
                    # print(r[1],r[2],r[3])
                    news.append(f"{str(r[2].date())} - [{r[1]}]({r[3]})")
                if "zhuzhou" in url:
                    news.append(f"{str(r[2].date())} - [{r[1]}]({r[3]})")
                    # news.append(str(r[2].date()) + " - " + r[1])
    if len(news) > 1:
        print("开始发送微信消息")
        title = "有新招聘信息啦"
        send_wechat_message(title, news)
    else:
        print("内容为空，不发送微信消息")


def get_html(url):
    title_set = ""
    date_set = ""
    url_set = ""
    file_name = ""
    file_path = ""
    retry_count = 5
    while retry_count > 0:
        try:
            proxy = get_proxy().get("proxy")
            resp = requests.get(url, headers=headers, proxies={"http": "http://{}".format(proxy)})
            resp.encoding = "utf-8"
            html = etree.HTML(resp.text)

            if "xiangtan" in url:
                print("收集湘潭新闻")
                title_set = html.xpath('//ul[@class="rchtg_tit"]//li//a')
                date_set = html.xpath('//ul[@class="rchtg_tit"]//li//span')
                url_set = html.xpath('//div[@class="rchtg fr"]//li//a/@href')
                file_name = "xiangtan.csv"
                file_path = os.path.join(abs_path, file_name)
            if "zhuzhou" in url:
                print("收集株洲新闻")
                title_set = html.xpath('//div[@class="gzdt fl"]//li//a')
                date_set = html.xpath('//div[@class="gzdt fl"]//li//span')
                url_set = html.xpath('//div[@class="gzdt fl"]//li//a/@href')
                file_name = "zhuzhou.csv"
                file_path = os.path.join(abs_path, file_name)

            if len(title_set) == 0:
                title = "爬取出错了，请检查"
                message_url = "https://sc.ftqq.com/SCU54509Tcad1c6121a054925bb4780cea07f3c355d1a1299a8bb2.send"
                requests.get(message_url + title)

            titles = []
            dates = []
            urls = []

            current_data = load_file(file_name)

            count = 0
            while count < len(title_set):
                if title_set[count].text.strip() not in current_data["news_title"].values:
                    titles.append(title_set[count].text.strip())
                    dates.append(date_set[count].text.strip()[1:-1])
                    urls.append(str(url_set[count]).strip())
                count += 1

            recruitment_dict = {
                "news_title": titles,
                "news_date": dates,
                "news_url": urls
            }

            recruitment_df = pd.DataFrame(recruitment_dict)

            if recruitment_df is not None:
                # 将日期列数据格式修改为Datetime
                recruitment_df["news_date"] = pd.to_datetime(recruitment_df.news_date)

                # 以日期排序
                recruitment_df.sort_values("news_date", inplace=True)
                recruitment_df = recruitment_df.reset_index(drop=True)
                if current_data.shape[0] != 0:
                    mode = "a"
                    header = 0
                else:
                    mode = "w"
                    header = 1
                write_csv(recruitment_df, file_path, mode, header)
            if recruitment_df.shape[0] == 0:
                recruitment_df = current_data
        except Exception as error:
            print(f"重试：{retry_count}, 错误：{error}")
            retry_count -= 1
            sleep_time = random.randint(2, 10)
            sleep(sleep_time)
        else:
            return recruitment_df


def load_file(filename):
    file_path = os.path.join(abs_path, filename)
    try:
        data = pd.read_csv(file_path, skip_blank_lines=True)
        data["news_date"] = pd.to_datetime(data.news_date)
        data.dropna()
    except Exception as error:
        print(error)
        data = pd.DataFrame(columns=["news_title", "news_date", "news_url"])
        write_csv(data, file_path, mode="w", header=1)
    return data


def write_csv(data, filename, mode, header):
    file_path = os.path.join(abs_path, filename)
    data.to_csv(file_path, mode=mode, header=header)


def send_wechat_message(title, content):
    message_urls = ["https://sc.ftqq.com/SCU54509Tcad1c6121a054925bb4780cea07f3c355d1a1299a8bb2.send",
                    "https://sc.ftqq.com/SCU65330Td3644fdefb6f2c0c78b3c6d1ea5832225db8c49f4c50b.send"]
    message_title = title
    post_params = {
        "text": message_title,
        "desp": "\n\r".join(content)
    }
    # print(json.dumps(post_params))
    for url in message_urls:
        result = requests.post(url, data=post_params)
        result.encoding = "utf-8"
        print(result.text)


if __name__ == '__main__':
    process_urls(url_list)
