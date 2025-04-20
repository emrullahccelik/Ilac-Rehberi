import pandas as pd
import requests
import os
import tempfile
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
from pdf2image import convert_from_bytes

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_PATH = r"C:\Program Files\poppler-24.08.0\Library\bin"


def ocr_pdf_from_url(url):
    for i in range(1,3):
        try:
            response = requests.get(url)
            images = convert_from_bytes(response.content, poppler_path=POPPLER_PATH)

            full_text = ""
            for image in images:
                text = pytesseract.image_to_string(image, lang='tur')  # Türkçe için
                full_text += text + "\n"

            return full_text
        except Exception as e:
            print("request error trying again")

    return  False


if __name__ == '__main__':
    zerodf = pd.read_csv("data/textZero.csv", encoding='utf-8-sig')

    for index, row in zerodf.iterrows():
        try:

            filename = row["fileName"]
            filepath = row["filepath"]
            url = row["link"]
            print(f"proccessing {url} {filename} {filepath}")

            text = ocr_pdf_from_url(url)
            if text:
                with open(f"{filepath},{filename}", 'w', encoding='utf-8') as file:
                    file.write(text)
                print("success")
            else:
                print("no text")

        except Exception as e:
            print(e)

