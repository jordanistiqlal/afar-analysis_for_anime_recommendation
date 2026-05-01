
def scrape_data(username):
    import requests
    import pandas as pd
    from collections import OrderedDict
    import time
    from dotenv import load_dotenv 
    import os
    load_dotenv()

    start_time = time.time()
    
    # Gunakan API MAL (perlu client ID dari https://myanimelist.net/apiconfig)
    headers = {
        'X-MAL-CLIENT-ID': os.getenv('X_MAL_CLIENT_ID')
    }
    
    # API endpoint untuk user anime list
    url = f"{os.getenv('MAL-URL')}/v2/users/{username}/animelist"
    params = {
        'fields': 'list_status',
        'limit': 1000
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=15)
    if response.status_code != 200:
        return {"error": "Username not found or API error"}
    
    json_data = response.json()

    try:
        data = []
        
        for item in json_data.get('data', []):
            anime = item['node']
            status = item['list_status']

            watched = status.get('num_episodes_watched', 0)
            total = anime.get('num_episodes', '?')
            if status.get('status') != 'completed':
                progress = f"{watched}/{total}"
            else:
                progress = f"{watched}"
            
            data.append({
                'id': anime['id'],
                'image': anime['main_picture']['medium'] if 'main_picture' in anime else '',
                'title': anime['title'],
                'score': status.get('score', 0),
                'type': anime.get('media_type', ''),
                'Progress': progress
            })
        
        df = pd.DataFrame(data)

        elapsed_time = time.time() - start_time
        
        return OrderedDict({
            "username": username,
            "total_items": len(df),
            "elapsed_time_sec": round(elapsed_time, 2),
            "data": df.to_dict(orient='records')
        })
        
    except Exception as e:
        return {"error": str(e)}