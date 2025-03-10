from tabulate import tabulate
import duckdb

import json
import pandas as pd

import requests
from pyquery import PyQuery as pq

import random

import rich

from openai import OpenAI
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


from ..settings import Settings
output_data_dir = Settings().get_test_ai_data_dir()


# -------------------- ENUMERATIONS --------------------

class CoreEconomicRole(str, Enum):
    """
    Top-level economic function of the company.
    This categorizes companies into broad economic roles.
    """
    PRODUCERS = "Producers of Physical Goods"
    SERVICES = "Service Providers"
    DIGITAL_AI = "Digital & AI-Centric Enterprises"
    INFRASTRUCTURE = "Infrastructure & Logistics"
    FINANCE = "Finance & Capital Markets"
    GOVERNANCE = "Governance & Nonprofit Institutions"

class CompanySize(str, Enum):
    """
    Describes the size of a company based on its revenue or market capitalization.
    """
    SMALL = "Small"
    MEDIUM = "Medium"
    LARGE = "Large"

class AIIntegrationLevel(str, Enum):
    """
    Describes the degree to which AI is integrated into a company's business model.
    """
    AI_NATIVE = "AI-Native"
    AI_ENHANCED = "AI-Enhanced"
    AI_AGNOSTIC = "AI-Agnostic"


class HerfindahlHirschmanIndex(BaseModel):
    """
    Represents the Herfindahl-Hirschman Index (HHI) for market concentration.
    
    Attributes:
        value: The calculated HHI value for market concentration.
        description: A brief explanation of the HHI value.
    """
    value: int 
    
    
# -------------------- LEVEL 3: SUBSECTOR MODEL --------------------

class Subsector(BaseModel):
    """
    Represents a highly specific area of business activity.
    
    Attributes:
        name: The name of the subsector (e.g., "AI-Driven Hedge Funds & Algorithmic Trading").
        description: A detailed explanation of this subsector.
        example_companies: Optional list of example companies operating in this subsector.
    """
    role: CoreEconomicRole
    sector: str 
    sub_sector: str 
    description: str

    hh_index : HerfindahlHirschmanIndex 

# -------------------- LEVEL 2: INDUSTRY FUNCTION MODEL --------------------

class Sector(BaseModel):
    """
    Represents a general category within an economic role.
    
    Attributes:
        name: The name of the industry function (e.g., 'Investment & Wealth Management').
        description: A high-level description of the industry function.
        subsectors: A list of subsectors that further specialize the industry function.
    """
    role: CoreEconomicRole
    sector: str 
    description: str 
    
    hh_index : HerfindahlHirschmanIndex 


# -------------------- LEVEL 1: CORE ECONOMIC CATEGORY MODEL --------------------

class CoreEconomicCategory(BaseModel):
    """
    Represents the top-level economic role of a company.
    
    Attributes:
        role: The primary economic role (from CoreEconomicRole enum).
        industry_functions: A list of industry functions (Level 2) within this core role.
    """
    role: CoreEconomicRole 
    description: str
    # industry_functions: List[IndustryFunction] = Field(
    #     ...,
    #     description="List of industry functions within this core role."
    # )
    
    hh_index : HerfindahlHirschmanIndex
    


# -------------------- AI & DIGITAL MATURITY MODEL --------------------

class AIDigitalMaturity(BaseModel):
    """
    Represents how AI influences a company's operations.
    
    Attributes:
        level: The degree of AI integration (from AIIntegrationLevel enum).
        notes: Additional notes on how AI is used in the company.
    """
    level: AIIntegrationLevel = Field(..., description="Degree of AI integration in the business.")
    notes: Optional[str] = Field(
        None,
        description="Additional notes on how AI is used in the company."
    )


# -------------------- FINAL COMPANY CLASSIFICATION MODEL --------------------

class Company(BaseModel):
    role: CoreEconomicRole
    sector: str 
    sub_sector: str 
    company_name: str 
    ticker : str
    description: str
    ai_maturity: AIIntegrationLevel 
    company_size: CompanySize
    market_share_percent: float
    ceo: str
    num_employees: int
    company_history: str


# -------------------- LISTS  --------------------
class CoreEconomicCategories(BaseModel):
    categories: list[CoreEconomicCategory]

class Sectors(BaseModel):
    sectors: list[Sector]

class SubSectors(BaseModel):
    subsectors: list[Subsector]
    
class Companies(BaseModel):
    companies: list[Company]
    

def get_level_1_core_economic_categories() -> CoreEconomicCategory:
    client = OpenAI()
    
    system_prompt = f"The year is 2030. You are a helpful stock market analyst. The Herfindahl-Hirschman Index (HHI) scale ranges from 0 to 10,000. The HHI is a measure of market concentration, or how competitive a market is. The HHI scale is divided into three categories: Low market concentration: HHI is less than 1,500, Moderate market concentration: HHI is between 1,500 and 2,500, High market concentration: HHI is greater than 2,500"
    user_prompt = f"You are constructing a top-down industry classification system. Please generate a list of 7 core economic categories based on the current economic landscape."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=CoreEconomicCategories,
    )

    output = completion.choices[0].message.parsed

    return output



def get_level_2_sectors(list_of_core_economic_categories, core_economic_category) -> CoreEconomicCategory:
    
    client = OpenAI()

    system_prompt = f"The year is 2030. You are a helpful stock market analyst. The Herfindahl-Hirschman Index (HHI) scale ranges from 0 to 10,000. The HHI is a measure of market concentration, or how competitive a market is. The HHI scale is divided into three categories: Low market concentration: HHI is less than 1,500, Moderate market concentration: HHI is between 1,500 and 2,500, High market concentration: HHI is greater than 2,500. Here is a list of the core economic categories: {list_of_core_economic_categories}"
    user_prompt = f"You are constructing an industry classification system. Please generate a list of a reasonable number of sectors in the following core economic category based on the current economic landscape.: {core_economic_category}"

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=Sectors,
    )

    output = completion.choices[0].message.parsed

    return output


def get_level_3_subsectors(list_of_l2_sector, l2_sector) -> CoreEconomicCategory:
    
    client = OpenAI()

    system_prompt = f"The year is 2030. You are a helpful stock market analyst. The Herfindahl-Hirschman Index (HHI) scale ranges from 0 to 10,000. The HHI is a measure of market concentration, or how competitive a market is. The HHI scale is divided into three categories: Low market concentration: HHI is less than 1,500, Moderate market concentration: HHI is between 1,500 and 2,500, High market concentration: HHI is greater than 2,500. Here is a list of the sector categories: {list_of_l2_sector}"
    user_prompt = f"You are constructing an industry classification system. Please generate a list of a reasonable number of sub-sectors in the following sector based on the current economic landscape.: {l2_sector}"

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=SubSectors,
    )

    output = completion.choices[0].message.parsed

    return output


def get_level_4_companies(list_of_l3_sub_sectors, l3_sub_sector) -> CoreEconomicCategory:
    client = OpenAI()

    TV_SHOWS = [
        "Silicon Valley",
        "Mr. Robot",
        "Black Mirror",
        "Westworld",
        "The Expanse",
        "Altered Carbon",
        "Arrested Development",
        "The Office",
        "The IT Crowd",
        "Archer",
        "The Simpsons",
        "Futurama",
        "Better Call Saul",
    ]
    
    # select a random TV show
    tv_show = random.choice(TV_SHOWS)
    
    system_prompt = f"You are a helpful assistant, researching a novel about fictitious corporations, in the style of {tv_show}, set in the year is 2030. Here is a description of the industry sub-sector:  {list_of_l3_sub_sectors}"
    user_prompt = f"Please generating an entertaining and informative list of fictional companies in the following sub-sector {l3_sub_sector}"

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=Companies,
    )

    output = completion.choices[0].message.parsed

    return output




def save_L1_ai_classification_model():
    l1 = get_level_1_core_economic_categories()
    rich.print(l1.model_dump_json(indent=4))
    
    # save JSON to file to output_data_dir
    with open(output_data_dir / 'L1_categories.json', 'w') as f:
        f.write(l1.model_dump_json(indent=4))

def load_L1_ai_classification_model():
    file = output_data_dir / 'L1_categories.json'
    l1 = json.loads(file.read_text())
    
    return l1


#get_level_2_sectors
def save_L2_ai_classification_model():
    # load the L1 categories
    file = output_data_dir / 'L1_categories.json'
    l1 = json.loads(file.read_text())

    rich.print(l1['categories'])
    
    # iterate over the L1 categories
    for category in l1['categories']:
        l2 = get_level_2_sectors(l1, category)
        rich.print(l2.model_dump_json(indent=4))
        
        # save JSON to file to output_data_dir
        with open(output_data_dir / f"L2_{category['role']}.json", 'w') as f:
            f.write(l2.model_dump_json(indent=4))

def load_L2_ai_classification_model(category):
    file = output_data_dir / f"L2_{category['role']}.json"
    l2 = json.loads(file.read_text())    
    return l2

#get_level_2_sectors
def save_L3_ai_classification_model():
    # load the L1 categories
    file = output_data_dir / 'L1_categories.json'
    l1 = json.loads(file.read_text())

    #rich.print(l1['categories'])
    
    # iterate over the L1 categories
    for category in l1['categories']:
        file = output_data_dir / f"L2_{category['role']}.json"
        l2 = json.loads(file.read_text())
        
        #rich.print(l2['sectors'])
        
        # iterate over the L2 sectors
        for sector in l2['sectors']:
            l3 = get_level_3_subsectors(l2, sector)
            rich.print(l3.model_dump_json(indent=4))
            
            # save JSON to file to output_data_dir
            with open(output_data_dir / f"L3_{sector['sector']}.json", 'w') as f:
                f.write(l3.model_dump_json(indent=4))

def load_L3_ai_classification_model(sector):
    file = output_data_dir / f"L3_{sector['sector']}.json"
    l3 = json.loads(file.read_text())
    #rich.print(l3)
    
    return l3

def gpt_L4_ai_companies():
    # load the L1 categories
    file = output_data_dir / 'L1_categories.json'
    l1 = json.loads(file.read_text())

    #rich.print(l1['categories'])
    
    # iterate over the L1 categories
    for category in l1['categories']:
        file = output_data_dir / f"L2_{category['role']}.json"
        l2 = json.loads(file.read_text())
        
        #rich.print(l2['sectors'])
        
        # iterate over the L2 sectors
        for sector in l2['sectors']:
            file = output_data_dir / f"L3_{sector['sector']}.json"
            l3 = json.loads(file.read_text())
            
            #rich.print(l3['subsectors'])
            
            # iterate over the L3 subsectors
            for subsector in l3['subsectors']:
                l4 = get_level_4_companies(l3, subsector)
                
                rich.print(l4.model_dump_json(indent=4))
                
                # save JSON to file to output_data_dir
                with open(output_data_dir / f"L4_{subsector['sub_sector']}.json", 'w') as f:
                    f.write(l4.model_dump_json(indent=4))

def load_L4_ai_companies(subsector):
    file = output_data_dir / f"L4_{subsector['sub_sector']}.json"
    l4 = json.loads(file.read_text())
    #rich.print(l4)
    
    return l4









class Level1IndustrySector(BaseModel):
    sector_id: int
    
    sector: str
    
    description: str

class Level1IndustrySectors(BaseModel):
    sectors: list[Level1IndustrySector]

class Level2IndustrySector(BaseModel):
    sector_id: int
    sub_sector_id: int
    
    sector: str
    sub_sector: str
    
    description: str

class Level2IndustrySectors(BaseModel):
    sectors: list[Level2IndustrySector]

class FictitiousCompany(BaseModel):
    sector_id: int
    sub_sector_id: int
    sector: str
    sub_sector: str

    ticker_symbol: str
    
    name: str
    
    description: str
    

class FictitiousCompanies(BaseModel):
    sector: str
    industry_sub_sector: str
    companies: list[FictitiousCompany]

    


# class TechnicalPattern(BaseModel):
#     date_range: str
#     pattern_type: str
#     commentary: str
#     sentiment: str

# class TechnicalAnalysis(BaseModel):
#     date_range: str
#     patterns: list[TechnicalPattern]

# def get_timeseries_analysis(ticker : str, date_start : str, date_end : str) -> TechnicalAnalysis:
#     db_file = Settings().get_db_file()
#     conn = duckdb.connect(db_file)

#     # get the security id
#     qry = f"SELECT * FROM securities WHERE base_ticker = '{ticker}'"
#     security = conn.execute(qry).fetch_df()
#     security_id = security.security_id[0]
#     name = security.name[0]

#     # get market data
#     qry = f"SELECT * FROM market_data WHERE security_id = '{security_id}' AND trade_date BETWEEN '{date_start}' AND '{date_end}'"
#     df = conn.execute(qry).fetch_df()

#     conn.close()

#     column_to_attribute = {
#         'trade_date': 'date',
#         'open_price': 'open',
#         'high_price': 'high',
#         'low_price': 'low',
#         'close_price': 'close',
#         'volume': 'volume',
#         'vwap': 'vwap'
#     }

#     # Rename columns
#     df.rename(columns=column_to_attribute, inplace=True)
#     df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'vwap']]

#     # print df using tabulate
#     print(tabulate(df, headers='keys', tablefmt='psql'))

#     system_prompt = f"You are a helpful stock market technical analyst focused on {ticker} ({name}). You are analyzing the stock market time-series data for a given date range, to identify predictable patterns and provide market commentary and analysis."
#     df_str = df.to_string(index=False)
#     user_prompt = f"Following is a time-series of the stock price: {df_str}. Please provide your analysis for the date range {date_start} to {date_end}."

#     completion = client.beta.chat.completions.parse(
#         model="gpt-4o-2024-08-06",
#         messages=[
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_prompt}
#         ],
#         response_format=TechnicalAnalysis,
#     )

#     output = completion.choices[0].message.parsed

#     return output



# class Hypothesis(BaseModel):
#     hypothesis: str
#     data: str

# class MarketCommentary(BaseModel):
#     comments: list[Hypothesis]
    


# def get_daily_market_news(date : str) -> MarketCommentary:
#     db_file = Settings().get_db_file()
#     conn = duckdb.connect(db_file)

#     # get the security id
#     qry = f"""
#         SELECT
#             base_ticker AS ticker, 
#             name,
#             ROUND(ccy_volume_mm,2),
#             ROUND(daily_return * 100,2) AS daily_return_pct
#         FROM
#             market_daily_returns
#         WHERE
#             trade_date = '{date}'
#         ORDER BY
#             trade_date,
#             ccy_volume_mm DESC
#         LIMIT 100
#         """
#     market_data = conn.execute(qry).fetch_df()

#     print(tabulate(market_data, headers='keys', tablefmt='psql'))

#     system_prompt = f"You are a helpful stock market analyst. You are analyzing the previous day's market returns, to identify predictable patterns and provide market commentary and analysis."
#     market_data_str = market_data.to_string(index=False)
#     user_prompt = f"Following is the market data: ```{market_data_str}```. Please provide your analysis."

#     completion = client.beta.chat.completions.parse(
#         model="gpt-4o-2024-08-06",
#         messages=[
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_prompt}
#         ],
#         response_format=MarketCommentary,
#     )

#     output = completion.choices[0].message.parsed

#     return output





# def get_news_summary_briefing() -> str:
#     url = 'https://en.wikipedia.org/wiki/Portal:Current_events'

#     headers = {
#         'User-Agent': 'dxdy v0.1.0'
#     }


#     http_response = requests.get(url, headers=headers)
#     html = http_response.text

#     d = pq(html)
#     x = d(".current-events-content")


#     system_prompt = "You are a helpful news analyst focused on providing a summary of current events. You are analyzing the current events data from Wikipedia, to provide a summary of the latest news in Markdown format."
#     user_prompt = f"Following is a summary of the latest news in HTML format: ```{x.html()}```. Please provide a brief summary of the latest news."

#     print(user_prompt)
#     completion = client.beta.chat.completions.parse(
#         model="gpt-4o-2024-08-06",
#         messages=[
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_prompt}
#         ]
#     )

#     output = completion.choices[0].message.parsed
#     print(output)
#     return output