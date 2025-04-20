from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import pandas as pd
import time

def trowDataScrapper(tr_element):
    td_elements = tr_element.find_all('td')

    kübDoc = td_elements[5].find('a', class_='badge')
    hrefKübDoc = kübDoc['href'] if kübDoc else None

    ktDoc = td_elements[6].find('a', class_='badge')
    hrefKtDoc = ktDoc['href'] if ktDoc else None

    trowDataJson = {
        "ilaçAdı": td_elements[0].text.strip(),
        "etkinMaddeAdı": td_elements[1].text.strip(),
        "firmaAdı": td_elements[2].text.strip(),
        "kübOnayTarihi": td_elements[3].text.strip(),
        "ktOnayTarihi": td_elements[4].text.strip(),
        "kübDocLink": hrefKübDoc,
        "ktDocLink": hrefKtDoc
    }

    return trowDataJson


# HTML içinde <tbody> kısmındaki verileri işler
def tbodyDataScrapper(tbodyHtml):
    soup = BeautifulSoup(tbodyHtml, 'html.parser')
    tr_elements = soup.find_all('tr')
    scraped_data = []

    for tr_element in tr_elements:
        json_data = trowDataScrapper(tr_element)
        scraped_data.append(json_data)

    return scraped_data


def scrape_titck_data(output_csv_path):
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 20)
    maxPageNum = 151  # Maksimum sayfa numarası

    data = []

    try:
        # Web sitesine gidin
        driver.get("https://www.titck.gov.tr/kubkt")
        postsLengthElement = wait.until(EC.presence_of_element_located((By.NAME, "posts_length")))

        time.sleep(3)

        # Her sayfada 100 veri olacak şekilde ayarla
        selectLength = Select(postsLengthElement)
        selectLength.select_by_value("100")
        time.sleep(3)

        while True:
            currentPageNum = int(driver.find_element(By.CLASS_NAME, "paginate_button.current").text)
            next_button = driver.find_element(By.ID, "posts_next")
            driver.execute_script("arguments[0].scrollIntoView();", next_button)

            print(f"{currentPageNum}. sayfa verisi çekiliyor.")
            tbody_element = driver.find_element(By.TAG_NAME, "tbody")
            tbodyHtml = tbody_element.get_attribute('outerHTML')
            pageData = tbodyDataScrapper(tbodyHtml)
            print(f"{len(pageData)} satır veri alındı.")

            data.extend(pageData)

            if currentPageNum == maxPageNum:  # Son sayfadaysak döngüyü sonlandır
                break

            driver.execute_script("arguments[0].click();", next_button)
            WebDriverWait(driver, 10).until(
                EC.staleness_of(next_button)  # Sayfa yenilendiğinde eski elementi bekler
            )

    finally:
        print("İşlem tamamlandı, tarayıcı kapatılıyor.")
        driver.quit()

    # Toplanan verileri bir DataFrame'e dök ve CSV dosyasına kaydet
    df = pd.DataFrame(data)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    print(f"Veriler {output_csv_path} dosyasına kaydedildi.")


if __name__ == "__main__":
    output_csv_path = "data/output.csv"  # Veri kaydedilecek CSV dosyası yolu
    scrape_titck_data(output_csv_path)
