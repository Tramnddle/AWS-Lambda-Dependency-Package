

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import dns
from io import StringIO

def my_handler(event, context):
    Today = datetime.date.today()
    Today_str = Today.strftime("%Y-%m-%d")
    
    # Your existing code starts here
    
    # Send a GET request to the URL
    url = "https://fbref.com/en/comps/20/Bundesliga-Stats"
    response = requests.get(url)
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find all h1 tags
    h1_tags = soup.find_all("h1")
    
    # Iterate through the h1 tags to find the desired line
    for h1_tag in h1_tags:
        if "Bundesliga Stats" in h1_tag.text:
            line = h1_tag.text.strip()
            break
    
    year = line.split("-")[1].split()[0]
    all_matches = []
    
    standings_url = "https://fbref.com/en/comps/20/Bundesliga-Stats"
    data = requests.get(standings_url)
    soup = BeautifulSoup(data.text, features ='lxml')
    standings_table = soup.select('table.stats_table')[0]
    
    links = [l.get("href") for l in standings_table.find_all('a')]
    links = [l for l in links if '/squads/' in l]
    team_urls = [f"https://fbref.com{l}" for l in links]
        
    previous_season = soup.select("a.prev")[0].get("href")
    standings_url = f"https://fbref.com{previous_season}"
        
    for team_url in team_urls:
        team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        data = requests.get(team_url)
        html_data = StringIO(data.text)
        Scores_Fixtures = pd.read_html(html_data, match="Scores & Fixtures")[0]
        Scores_Fixtures = Scores_Fixtures[['Date','Time','Comp','Round','Day','Venue','GF','GA','Opponent','Poss']]
        Scores_Fixtures = Scores_Fixtures[(Scores_Fixtures['Date'] <= Today_str) & (Scores_Fixtures['Date'] > (Today - datetime.timedelta(days=7)).strftime("%Y-%m-%d"))]

        soup = BeautifulSoup(data.text, features ='lxml')
        links = [l.get("href") for l in soup.find_all('a')]
        links = [l for l in links if l and 'all_comps/shooting/' in l]
        data = requests.get(f"https://fbref.com{links[0]}")
        html_data = StringIO(data.text)
        shooting = pd.read_html(html_data, match="Shooting")[0]
        shooting.columns = shooting.columns.droplevel()
        shooting = shooting[(shooting['Date']<= Today_str) & (shooting['Date']> (Today- datetime.timedelta(days=7)).strftime("%Y-%m-%d"))]
            
        soup = BeautifulSoup(data.text, features ='lxml')
        links = soup.find_all('a')
        links = [l.get("href") for l in links]
        links = [l for l in links if l and 'all_comps/keeper/' in l]
        data = requests.get(f"https://fbref.com{links[0]}")
        html_data = StringIO(data.text)
        goalkeeping = pd.read_html(html_data, match="Goalkeeping")[0]
        goalkeeping.columns = goalkeeping.columns.droplevel()
        goalkeeping = goalkeeping[(goalkeeping['Date']<= Today_str) & (goalkeeping['Date']> (Today- datetime.timedelta(days=7)).strftime("%Y-%m-%d"))]
            
        #try:
        team_data = Scores_Fixtures.merge(shooting[['Date','Sh']], on = ['Date'])
        team_data = team_data.merge(goalkeeping[['Date','Save%']], on = ['Date'])
    
        #except ValueError:
        #continue
        team_data = team_data[team_data["Comp"] == "Bundesliga"]
            
        team_data["Season"] = year
        team_data["Team"] = team_name
        all_matches.append(team_data)
        time.sleep(20)
    
    match_df = pd.concat(all_matches)
    match_df.columns = [c.lower() for c in match_df.columns]
    
    match_df[['gf', 'ga','poss','sh','save%']] = match_df[['gf', 'ga','poss','sh','save%']].astype(float)
    match_df['season'] = match_df['season'].astype(int)
    
    # Your existing code ends here
    
    # Insert code to upload to GCS
    gcp_sa_credentials={
    "type": "service_account",
    "project_id": "footbalprediction-414107",
    "private_key_id": "ccae027241adbd582a4b8c05a4d530ab9089327a",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCl6OkIEh6U3ES4\nLB6MU4O8rlGmHlqhCd1Hq+n+47y5DAbslF1396D9hIBCRpZGbQx5P1CPg4ZHwJq9\nHSXnk1KJF39gHd8emG+yE9A/vWFcuTvTPbU7VpX93yFMAzyFoMRjxoQsP1bE3Zl5\n9Ic1/zxbJwWxHOfbxiil9+94IxhnIXmHdYb7dFkkGQuipBs7Dtoy0Pshs4Wlf0ge\nWGeGRCVq5ayEUXsXIeBZUzOXe82831AmTKHmEV8vmawg0oSBuBRw2zrbTXknlP36\nuASn5I/Tdwh0K6qwb+79uda/Fd6W8u7OD26OTYrWWwP9OxTrfSY2rSlzdEVKEoCJ\ncZLRFFQLAgMBAAECggEAEgq5fgSxFFa9wa9z5PusqZyP9CTR4zhLkCCMUjPEJqAT\nCAV8V/BIWWQK1JTmK6Nuhf2SI6vVsut+3aOT/H0OA9WCfFp96c8Bj9fNai5h2O+r\nJ1DLFWMC7aLRn/lWyrX4arHrAVV5TfBGi+IWar37ph1LZlQdN05IcP/80noyjFMF\nQOhQmp4PySU0ICk2cPM49TEw+y20grzmjF8/eFWg1YUhyeQWeiANHPx2x8qC+qAi\n1Aq3ANT8den41NHo2sSxSphvjNE30fQtgCiTheeWlBzXeFPiMbyhO8AYwG6migXW\nnmxWE0WMJ6GL0L6Jl207MVm4st5Pur0Xenv8yww8DQKBgQDSFJm97zceU00DCXAr\nFnrDlYruqYWEXJ62A8+l6kCUNwtvCqt5oORAkygUC8kp3pPIy8fSIbat3ldtrG1f\nk7v7jRBuYu8Pxl5JLO147+6tf09h366ngyquGnOJ9VjhFnvwu1cgNrJe/lTXTT3y\na6mn5Wkk2qoDkOZUvbDn1opktwKBgQDKLKrRtoz4jwPL6l2BqET4d9O5qML3hC3n\nR45V7CxUvKUxOz8zI2y/1PV/BoF6pQDpTwEz9C1KXdt6dJuir+sUqKcbPCTwlCGW\nK2Q4L5uiXWtVhm/Jb6h3mWFOYSWiSACG/KwcbTteI2JfZZpB3YHlQcTgEwiOthiF\nTSC3La0/TQKBgFVUwM4BaKYMt+9P2hvmWZ8wEuq2OOF2rZDJI4MFD44kfaRw9Q3G\naHBCVbkuwFsdaXHaNCQKRaWB9ok5zINSAr0+oznzPZ9ut8WJVjwVWSFn4NqkfNDV\n2nQ1klCrM5raAyXZMp6HGRS0wcliOpNJX/QunvK1TvF73dL16fGBl10pAoGAZgOl\n6g3wEhev9bv7lMoAi1ODbUI/pr92niYYJzj1oYhS3oWjvT0Zya4+desleGo8DH3G\nAJ4sIEM91Qtz4OJdf14ee/qcNRy52dlpR4SWRpZW65/bVkxWOIsXc4JHiBxGz9Y9\nTla7xyOZpsNQ0/1eZv0Jx3szLTerJdAmOuf8bF0CgYAXxmiCvTBJChjSqSaiygBP\ntCUTeuXGU4arXHP9HtoUxx/pfX4b/y9Zx6aBeVmvEeaG1GzPOPQY5Sq2jcbFZZSI\n5FVdZUjrdNS++UaI+/brkr2nC58Q+oBJ5Q8XTBRXzMdTaaIiQe3rMJkOCTQ7Jo6n\ncW8+T0q3yU7XfgBPjztESg==\n-----END PRIVATE KEY-----\n",
    "client_email": "streamlit-python-0410@footbalprediction-414107.iam.gserviceaccount.com",
    "client_id": "114047689527607639710",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/streamlit-python-0410%40footbalprediction-414107.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"}

    project_id = gcp_sa_credentials["project_id"]
    credentials = service_account.Credentials.from_service_account_info(gcp_sa_credentials)
    client = dns.Client(project=project_id, credentials=credentials)
    
    client = storage.Client()
    bucket = client.get_bucket('bundesliga_0410')
    blob = bucket.blob('matches.csv')
    blob.download_to_filename('matches.csv')
    df = pd.read_csv('matches.csv')
    
    df[['gf', 'ga','poss','sh','save%']] = df[['gf', 'ga','poss','sh','save%']].astype(float)
    df['season'] = df['season'].astype(int)
    df = df.drop('index', axis =1)
    
    df_combined = pd.concat([df, match_df]).drop_duplicates().reset_index()
    
    updated_df = df_combined.to_csv(index=False)
    filename_in_bucket = bucket.blob('matches.csv')
    filename_in_bucket.upload_from_string(updated_df)
    
    return "Data uploaded successfully to Google Cloud Storage", 200, {"Content-Type": "text/plain"}

