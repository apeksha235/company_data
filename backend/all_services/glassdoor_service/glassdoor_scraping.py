import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import math
import logging
import re
import os
from concurrent.futures import ThreadPoolExecutor
from ..shared.error_handling_middleware import ErrorHandlingMiddleware
from .proxy_middleware import ProxyMiddleware

logging.basicConfig(
    filename="glassdoor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
proxy = "http://sp76y0ei7t:kelp123@all.dc.smartproxy.com:10000"
proxy_middleware = ProxyMiddleware(proxy)
error_handling_middleware = ErrorHandlingMiddleware(retries=3, delay=5)
request_handler = error_handling_middleware

def glassdoor_func_wrapper(url, company_name, cin_number, source, title, count):
    try:
        if "Reviews" in url:
            return glassdoor_func_with_reviews(url, company_name, cin_number, source, title, count)
        else:
            return glassdoor_func_without_reviews(url, company_name, cin_number, count)
    except Exception as e:
        logging.error(f"Error processing {company_name}: {str(e)}")
        return None

def process_page(search_url):
    html_content = request_handler.process_request(search_url)
    if html_content is None:
        logging.critical("Request failed after all retries.")
        return None
    soup = BeautifulSoup(html_content, "html.parser")
    review = soup.find_all("a", class_="reviewLink")
    review_title = []
    for i in review:
        review_title.append(i.text)
    date_list = []
    position_list = []
    current = []
    experience = []
    date_pos = soup.find_all(class_="middle common__EiReviewDetailsStyle__newGrey")
    curr_exp = soup.find_all(class_="pt-xsm pt-md-0 css-1qxtz39 eg4psks0")
    for i in date_pos:
        d = i.text
        date, position = d.split("-", 1)
        date_list.append(date.strip())
        position_list.append(position.strip())
    for i in curr_exp:
        if "," in i.text:
            curr, exp = i.text.split(",")
            current.append(curr.strip())
            experience.append(exp.strip())
        else:
            current.append(i.text)
            experience.append("")
    location_elements = soup.select(".common__EiReviewDetailsStyle__newUiJobLine")
    locations = []
    for element in location_elements:
        if element.select(".middle span"):
            location = element.select(".middle span")[0].text.strip()
        else:
            location = " "
        locations.append(location)
    pros_type = soup.find_all("span", attrs={"data-test": "pros"})
    pros = [span.text.strip() for span in pros_type]
    cons_type = soup.find_all("span", attrs={"data-test": "cons"})
    cons = [span.text.strip() for span in cons_type]

    per2 = {'recommendation_to_a_friend': "", 'ceo_approval': "", 'positive_business_outlook': ""}
    svg_classes = {
        'SVGInline css-1kiw93k d-flex': "0",
        'SVGInline css-hcqxoa d-flex': "1",
        'SVGInline css-10xv9lv d-flex': "",
        'SVGInline css-1h93d4v d-flex': ""
    }
    review_categories = {
        'recommendation_to_a_friend': 'Recommend',
        'ceo_approval': 'CEO Approval',
        'positive_business_outlook': 'Business Outlook'
    }

    rec_list = []
    ceo_list = []
    bus_list = []
    for outer_div in soup.find_all('div', class_='d-flex my-std reviewBodyCell recommends css-1y3jl3a e1868oi10'):
        for div in outer_div.find_all('div', class_='d-flex align-items-center mr-std'):
            for svg_class, val in svg_classes.items():
                svg = div.find('span', class_=svg_class)
                if svg:
                    span = svg.find_next_sibling('span', class_='common__EiReviewDetailsStyle__newGrey')
                    if span:
                        for key, value in review_categories.items():
                            if span.text == value:
                                per2[key] = val
        rec = per2["recommendation_to_a_friend"]
        ceo = per2['ceo_approval']
        bus = per2['positive_business_outlook']
        rec_list.append(rec)
        ceo_list.append(ceo)
        bus_list.append(bus)
    return (
        review_title,
        date_list,
        position_list,
        locations,
        pros,
        cons,
        current,
        experience,
        rec_list,
        ceo_list,
        bus_list
    )


def glassdoor_func_with_reviews(url, company_name, cin_number, source, title, count):
    html_content = request_handler.process_request( url)  
    if html_content is None:
        logging.critical("Request failed after all retries.")
        return None
    soup = BeautifulSoup(html_content, "html.parser")
    div_element = soup.find('div', class_='d-flex justify-content-between justify-content-sm-around mx-std')
    links = div_element.find_all('a')
    second_link = links[1]
    url_link = str(second_link['href'])
    url=f"https://www.glassdoor.co.uk{url_link}"
    print(url)
    html_content = request_handler.process_request(url)  
    if html_content is None:
        logging.critical("Request failed after all retries.")
        return None
    soup = BeautifulSoup(html_content, "html.parser")
    all_review_title = []
    all_date_list = []
    all_position_list = []
    all_locations = []
    all_pros = []
    all_cons = []
    all_current = []
    all_experience = []
    all_rec=[]
    all_ceo=[]
    all_bus=[]
    time.sleep(3)
    ratings = {}
    ratings_col = [
        "rating",
        "culture",
        "diversity",
        "work_life_balance",
        "senior_management",
        "salary_benefits",
        "career_opportunities",
    ]

    per = [
        "individual_recommendation_to_a_friend",
        "individual_ceo_approval",
        "individual_positive_business_outlook",
    ]
    percentage = {}

    for i in range(len(per)):
        percentage[per[i]] = ""   

    for i in range(len(ratings_col)):
        ratings[ratings_col[i]] = ""

    rating_2 = {}
    rate = ratings_col = [
        "individual_rating",
        "individual_culture",
        "individual_diversity",
        "individual_work_life_balance",
        "individual_senior_management",
        "individual_salary_benefits",
        "individual_career_opportunities",
    ]
    for i in range(len(ratings_col)):
        rating_2[rate[i]] = ""
    span_tag = soup.find_all('span')
    total_reviews=0
    for i in span_tag:
     strong_tag = i.find('strong')
     if strong_tag:
      total_reviews = int(strong_tag.text.replace(',', ''))
      break
    print(company_name)
    print(total_reviews)
    review_col=total_reviews
    if total_reviews>5000:
      review_col=5000
    review_col=20
    pages = math.ceil(review_col / 10)
    for page in range(1, pages + 1):
        if page == 1:
            search_url = url+'?sort.sortType=RD&sort.ascending=false&filter.iso3Language=eng'
        else:
            search_url = url.replace(".htm", f"_P{page}.htm?sort.sortType=RD&sort.ascending=false&filter.iso3Language=eng'")
        result = process_page(search_url)
        if result is None:
            continue
        review_title, date_list, position_list, locations, pros, cons, current, experience,rec,ceo,bus = result
        all_review_title.extend(review_title)
        all_date_list.extend(date_list)
        all_position_list.extend(position_list)
        all_locations.extend(locations)
        all_pros.extend(pros)
        all_cons.extend(cons)
        all_current.extend(current)
        all_experience.extend(experience)
        all_rec.extend(rec)
        all_ceo.extend(ceo)
        all_bus.extend(bus)
    data = {}
    lang = "en"
    data["url"] = [url] * len(all_review_title)
    data["company_name"] = [company_name] * len(all_review_title)
    data["cin_number"] = [str(cin_number).zfill(8)] * len(all_review_title)
    for key, value in ratings.items():
        data[key] = [value] * len(all_review_title)
    for key, value in percentage.items():
        data[key] = [value] * len(all_review_title)
    data["ceo_rating_count"] = [""] * len(all_review_title)
    data["ceo_name"] = [""] * len(all_review_title)
    data["reviews_count"] = [str(total_reviews)] * len(all_review_title)
    data["reviews_collected"]=[str(len(all_review_title))]*len(all_review_title)
    data["english_review_count"] = [str(total_reviews)] * len(all_review_title)
    data["review_title_raw"] = all_review_title
    data["review_title"] = all_review_title
    data["review_title_language"] = [lang] * len(all_review_title)
    data["review_date"] = all_date_list
    data["employment_status_raw"] = all_current
    data["employment_status"] = all_current
    data["employment_status_language"] = [lang] * len(all_review_title)
    data["experience_raw"] = all_experience
    data["experience"] = all_experience
    data["experience_language"] = [lang] * len(all_review_title)
    data["designation_raw"] = all_position_list
    data["designation"] = all_position_list
    data["designation_language"] = [lang] * len(all_review_title)
    data["location_raw"] = all_locations
    data["location"] = all_locations
    data["location_language"] = [lang] * len(all_review_title)
    data["pros_raw"] = all_pros
    data["pros"] = all_pros
    data["pros_language"] = [lang] * len(all_review_title)
    data["cons_raw"] = all_cons
    data["cons"] = all_cons
    data["cons_language"] = [lang] * len(all_review_title)
    data["management_advice_raw"] = [""] * len(all_review_title)
    data["management_advice"] = [""] * len(all_review_title)
    data["management_advice_language"] = [lang] * len(all_review_title)
    data["recommendation_to_a_friend"]=all_rec
    data["ceo_approval"]=all_ceo
    data["positive_business_outlook"]=all_bus
    for key, value in rating_2.items():
        data[key] = [value] * len(all_review_title)
    if total_reviews > 0:
        (
            review_title,
            date_list,
            position_list,
            locations,
            pros,
            cons,
            current,
            experience,
            rec_list,
            ceo_list,
            bus_list
        ) = process_page(url)
        return (
            review_title,
            date_list,
            position_list,
            locations,
            pros,
            cons,
            current,
            experience,
            rec_list,
            ceo_list,
            bus_list
        )
    else:
        logging.warning(f"No reviews found for {company_name} - {cin_number}")
        return None


def glassdoor_func_without_reviews(url, company_name, cin_number, count):
    html_content = request_handler.process_request(url) 
    if html_content is None:
        logging.critical("Request failed after all retries.")
        return None
    soup = BeautifulSoup(html_content, "html.parser")
    div_element = soup.find('div', class_='d-flex justify-content-between justify-content-sm-around mx-std')
    links = div_element.find_all('a')
    second_link = links[1]
    url_link = str(second_link['href'])
    url=f"https://www.glassdoor.co.uk{url_link}"
    result = glassdoor_func_with_reviews(url, company_name, cin_number, count)
    
    if result is None:
        logging.warning(f"No reviews found for {company_name} - {cin_number}")
        return None

    (
        review_title,
        date_list,
        position_list,
        locations,
        pros,
        cons,
        current,
        experience,
        rec_list,
        ceo_list,
        bus_list
    ) = result
    
    return (
        review_title,
        date_list,
        position_list,
        locations,
        pros,
        cons,
        current,
        experience,
        rec_list,
        ceo_list,
        bus_list
    )
    
def fetch_reviews(companies_df, batch_size=10):
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = []
        for _, row in companies_df.iterrows():
            url = row["glassdoor_url"]
            company_name = row["company_name"]
            cin_number = row["cin_number"]
            source = row["source"]
            title = row["title"]
            count = row["review_count"]

            result = executor.submit(glassdoor_func_wrapper, url, company_name, cin_number, source, title, count)
            results.append(result)

        updated_df = pd.DataFrame(columns=companies_df.columns)  

    for i, result in enumerate(results):
        reviews = result.result()
        if reviews is not None:
            (
                review_title,
                date_list,
                position_list,
                locations,
                pros,
                cons,
                current,
                experience,
                rec_list,
                ceo_list,
                bus_list,
            ) = reviews
            row_data = {
                "review_title": review_title,
                "date_list": date_list,
                "position_list": position_list,
                "locations": locations,
                "pros": pros,
                "cons": cons,
                "current": current,
                "experience": experience,
                "rec_list": rec_list,
                "ceo_list": ceo_list,
                "bus_list": bus_list,
            }
            updated_row_df = pd.DataFrame(row_data)
            print(updated_row_df)
            updated_df = pd.concat([updated_df, updated_row_df], ignore_index=True)


    return updated_df


def main(url=None, csv_file=None):
    if url:
        data = {
            "company_name": ["Company"],
            "cin_number": ["1234567890"],
            "glassdoor_url": [url],
            "source": ["source"],
            "title": ["Title"],
            "review_count": [0],
        }

        companies_df = pd.DataFrame(data)
        df=fetch_reviews(companies_df)
        df.to_csv("company.csv",index=False)
    elif csv_file:
        companies_df = pd.read_csv(csv_file)
        df=fetch_reviews(companies_df)
        df.to_csv("company.csv",index=False)
    else:
        print("No URL or CSV file provided.")



if __name__ == "__main__":
    main()