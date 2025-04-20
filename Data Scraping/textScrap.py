import pandas as pd
import numpy as np
import requests
from tqdm import tqdm
import uuid
import PyPDF2
import pdfplumber
from io import BytesIO
import time
import random
import concurrent.futures



data = pd.read_csv("data/output.csv", encoding='utf-8-sig')
print(data.shape)


kübTxtFilePath="data/küb/"
ktTxtFilePath="data/kt/"

textZero = []
connError = []
timeout =20

headers = {
        "User-Agent": "Safari/537.3",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Connection": "keep-alive"
    }
def scrapTextKub(row, retry=3):
    global textZero, connError
    url = row["kübDocLink"]
    filename = row["fileName"]
    filepath = kübTxtFilePath

    if url:
        for i in range(1,retry+1):
            try:
                response = requests.get(url,timeout=timeout,headers=headers)
                if response.status_code == 200:
                    pdf_file = BytesIO(response.content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ''

                    for page_num in range(len(pdf_reader.pages)):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text()

                    if len(text) > 0:
                        with open(filepath + filename, 'w', encoding='utf-8') as text_file:
                            text_file.write(text)
                    else:
                        print("zero text ",{url})
                        textZero.append({
                            "fileName": filename,
                            "filepath": kübTxtFilePath,
                            "link": url
                        })
                    break
                else:
                    raise Exception(f"status code error {response.status_code}")
            except Exception as e:
                print(f"error getting data retry {i} {url} ",e)
                time.sleep(2)
                if i == retry:
                    print("no more retry !!!!!!")
                    connError.append({
                        "fileName": filename,
                        "filepath": kübTxtFilePath,
                        "link": url,
                        "error":e
                    })
                    break





def scrapTextKt(row, retry=3):
    global textZero, connError
    url = row["ktDocLink"]
    filename = row["fileName"]
    filepath = ktTxtFilePath

    if url:
        for i in range(1,retry+1):
            try:
                response = requests.get(url,timeout=timeout,headers=headers)
                if response.status_code == 200:
                    pdf_file = BytesIO(response.content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ''

                    for page_num in range(len(pdf_reader.pages)):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text()

                    if len(text) > 0:
                        with open(filepath + filename, 'w', encoding='utf-8') as text_file:
                            text_file.write(text)
                    else:
                        print("zero text ",{url})
                        textZero.append({
                            "fileName": filename,
                            "filepath":ktTxtFilePath,
                            "link": url
                        })
                    break
                else:
                    raise Exception(f"status code error {response.status_code}")
            except Exception as e:
                print(f"error getting data retry {i} {url} ",e)
                time.sleep(2)
                if i == retry:
                    print("no more retry :(")
                    connError.append({
                        "fileName": filename,
                        "filepath": ktTxtFilePath,
                        "link": url,
                        "error":e
                    })
                    break


if __name__ == '__main__':
    data=data[:100]
    print(data.shape)

    data["fileName"] = ""
    data["fileName"] = [f"{uuid.uuid4().hex}.txt" for _ in range(len(data))]
    print(data.shape)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        list(tqdm(executor.map(scrapTextKub, [row for _, row in data.iterrows()]), total=len(data),
                  desc="kub İndiriliyor..."))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        list(tqdm(executor.map(scrapTextKt, [row for _, row in data.iterrows()]), total=len(data),
                  desc="kt İndiriliyor..."))

    data.to_csv("data/finalOutput.csv", index=False, encoding='utf-8-sig')

    textZerodf = pd.DataFrame(textZero)
    textZerodf.to_csv("data/textZero.csv", index=False, encoding='utf-8-sig')

    connErrorodf = pd.DataFrame(connError)
    connErrorodf.to_csv("data/connError.csv", index=False, encoding='utf-8-sig')