from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from bs4.element import Tag

# create instance of webdriver
driver = webdriver.Chrome('C:/Users/VAMA/Downloads/chromedriver_win32/chromedriver.exe')
url = 'https://www.google.com'
driver.get(url)

# set keyword
keyword = 'outsourcing projects to india'
# we find the search bar using it's name attribute value
searchBar = driver.find_element_by_name('q')
# first we send our keyword to the search bar followed by the ent
searchBar.send_keys(keyword)
searchBar.send_keys('\n')

def scrape():
   pageInfo = []
   try:
      # wait for search results to be fetched
      WebDriverWait(driver, 10).until(
      EC.presence_of_element_located((By.CLASS_NAME, "g"))
      )
    
   except Exception as e:
      print(e)
      driver.quit()
   # contains the search results
   searchResults = driver.find_elements_by_class_name('g')
   for result in searchResults:
       element = result.find_element_by_css_selector('a')
       link = element.get_attribute('href')
       header = result.find_element_by_css_selector('h3').text
       text = result.find_element_by_class_name('IsZvec').text
       pageInfo.append({
           'header' : header, 'link' : link, 'text': text
       })
   return pageInfo

# Number of pages to scrape
numPages = 10
# All the scraped data
infoAll = []
# Scraped data from page 1
infoAll.extend(scrape())

for i in range(0 , numPages - 1):
   nextButton = driver.find_element_by_link_text('Next')
   nextButton.click()
   infoAll.extend(scrape())

print(infoAll)
