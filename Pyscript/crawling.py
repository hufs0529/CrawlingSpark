import time
import pandas as pd
import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium import webdriver


url = 'https://www.yogiyo.co.kr/mobile/#/286059/'

driver = webdriver.Chrome()
driver.maximize_window()
driver.get(url)
button = driver.find_element(By.CSS_SELECTOR, '#content > div.restaurant-detail.row.ng-scope > div.col-sm-8 > ul > li:nth-child(2)')
time.sleep(3)
button.click()
time.sleep(3)
df = pd.DataFrame(columns=["아이디", "날짜", "맛평점", "양평점", "배달평점", "리뷰"])


more_button = driver.find_element(By.XPATH, '//*[@id="review"]/li[12]/a')
SCROLL_PAUSE_TIME = 3
cnt = 0
# Get scroll height
last_height = driver.execute_script("return document.body.scrollHeight")        

while True:
  # Scroll down to bottom                                                      
  driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

  # Wait to load page
  time.sleep(SCROLL_PAUSE_TIME) 
  more_button.click()    
  print('click')                                         
  driver.execute_script("window.scrollTo(0, document.body.scrollHeight-50);")  
  time.sleep(SCROLL_PAUSE_TIME)
  cnt += 1
  if cnt == 259:
    break
  # Calculate new scroll height and compare with last scroll height           
  new_height = driver.execute_script("return document.body.scrollHeight")

  if new_height == last_height:                                                
    break

  last_height = new_height

html = driver.page_source

html = BeautifulSoup(html, 'html.parser')
review_box = html.find_all("li", class_= "list-group-item star-point ng-scope")

for review in review_box:
  # 날짜
  try:
    now = datetime.datetime.now()
    now_year = now.strftime('%Y')
    
    if now_year == '어제':
      date = review.find("span", class_="review-time ng-binding").text   
    
    # '어제'데이터만 크롤링한다 
    else:
      date = '-'
  except:
    date = '-'   
  
  # ID
  try:
    userID = review.find("span", class_="review-id ng-binding").text
    print(userID)
  except:
    userID = '-'  

  # 평점
  try:
    stars = review.find_all("span", class_ = "points ng-binding")
    taste, quantity, delivery = stars[0].text, stars[1].text, stars[2].text
  except:
    taste = '-'
    quantity = '-'
    delivery = '-'
    avg_star = '-'
    
  # 리뷰
  try:
    comment = review.find("p", class_="ng-binding").text
    print(comment)
  except:
    comment = '-' 


  data = {'아이디': userID, '날짜': date, '맛평점': taste,  '양평점' : quantity,
                '배달평점': delivery, '리뷰': comment}
  #data = {'아이디': userID, '날짜': date, '평점': s, '리뷰': comment}


  # '어제'가 아닌 데이터는 '-'처리 되었다
  if date != '-':
    df = df.append(data, ignore_index=True)
  time.sleep(1)


df.to_csv(now + '.csv', encoding="utf-8-sig")
print(df)