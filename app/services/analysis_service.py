import gc

def analysis_anime(data):
    from app.services.dataset_store import get_dataset
    import pandas as pd

    df = pd.DataFrame(data)
    df.rename(columns={'id': 'mal_id'}, inplace=True)
    df.rename(columns={'score': 'my_score'}, inplace=True)

    if len(data) < 1:
        return {"genre": [],"studio": [],"producer": [],"demographic": [],"theme": [],"anime_time": [],"recommendation": []}

    dataset = get_dataset()

    df['mal_id'] = pd.to_numeric(df['mal_id'], errors='coerce')

    cols_to_drop = [col for col in df.columns if col in dataset.columns and col != 'mal_id']
    df_clean = dataset.drop(columns=cols_to_drop+['image_url'])

    merged_df = pd.merge(df, df_clean, on='mal_id', how='inner')
    merged_df = merged_df.drop(columns=['Unnamed: 0'], errors='ignore')

    analysis_df = merged_df.copy()

    genre = fetch_genre(analysis_df)
    studio = fetch_studio(analysis_df)
    producer = fetch_producer(analysis_df)
    demographic = fetch_demographic(analysis_df)
    theme = fetch_theme(analysis_df)
    anime_time = fetch_anime_time(analysis_df)

    result = {
        "genre": genre,
        "studio": studio,
        "producer": producer,
        "demographic": demographic,
        "theme": theme,
        "anime_time": anime_time
    }

    del df, df_clean, merged_df, analysis_df, dataset
    gc.collect()

    return result

def fetch_studio(df):
    studio_df = df.explode('studio')['studio'].value_counts().reset_index()
    studio_df.columns = ['studios', 'count']

    result = studio_df.to_dict(orient='records')

    del studio_df
    gc.collect()

    return result

def fetch_genre(df):
    genre_df = df.explode('genre')['genre'].value_counts().reset_index()
    genre_df.columns = ['genres', 'count']

    result = genre_df.to_dict(orient='records')

    del genre_df
    gc.collect()

    return result

def fetch_producer(df):
    producer_df = df.explode('producer')['producer'].value_counts().reset_index()
    producer_df.columns = ['producers', 'count']

    result = producer_df.to_dict(orient='records')

    del producer_df
    gc.collect()

    return result

def fetch_demographic(df):
    required_demographics = ['Shounen', 'Seinen', 'Shoujo', 'Josei', 'Kids']
    
    demographic_series = df.explode('demographic')['demographic']
    demographic_df = (
        demographic_series.value_counts()
        .reindex(required_demographics, fill_value=0)
        .reset_index()
    )
    demographic_df.columns = ['demographics', 'count']
    
    result = demographic_df.to_dict(orient='records')
    
    del demographic_series, demographic_df
    gc.collect()
    
    return result

def fetch_theme(df):
    theme_df = df.explode('theme')['theme'].value_counts().reset_index()
    theme_df.columns = ['themes', 'count']
    theme_df = theme_df[theme_df['themes'] != '-']

    result = theme_df.to_dict(orient='records')
    
    del theme_df
    gc.collect()
    
    return result

def fetch_anime_time(df):
    import pandas as pd

    temp_df = df[['premiered']].copy()
    temp_df['premiered'] = temp_df['premiered'].str.replace('  ', ' ', regex=False).str.strip()
    temp_df = temp_df[~temp_df['premiered'].isin(['-', '?'])]
    
    temp_df['season'] = temp_df['premiered'].str.extract(r'^(Fall|Spring|Summer|Winter)', expand=False)
    temp_df['year'] = temp_df['premiered'].str.extract(r'(\d{4})', expand=False)
    
    temp_df = temp_df.dropna(subset=['season', 'year'])
    count_df = temp_df.groupby(['year', 'season']).size().reset_index(name='count')
    
    season_order = ['Fall', 'Spring', 'Summer', 'Winter']
    full_index = pd.MultiIndex.from_product(
        [count_df['year'].unique(), season_order],
        names=['year', 'season']
    )
    
    final_df = (
        count_df.set_index(['year', 'season'])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    
    final_df['time'] = final_df['season'] + ' ' + final_df['year']
    final_df['season'] = pd.Categorical(
        final_df['season'],
        categories=season_order,
        ordered=True
    )
    
    final_df = final_df.sort_values(['year', 'season'])
    time_df = final_df[['time', 'count']].reset_index(drop=True)
    
    result = time_df.to_dict(orient='records')
    
    del count_df, final_df, time_df
    gc.collect()
    
    return result

def fetch_analysis(df):
    import pandas as pd
 
    from app.services.dataset_store import get_dataset
    from app.utils.features import build_feature_matrix_cached
    from app.services.recomendation_service import recommend_unwatched

    df = pd.DataFrame(df)
    df.rename(columns={'id': 'mal_id'}, inplace=True)
    df.rename(columns={'score': 'my_score'}, inplace=True)

    if len(df) < 1:
        return {"recommendation": []}

    dataset = get_dataset()

    df['mal_id'] = pd.to_numeric(df['mal_id'], errors='coerce')

    cols_to_drop = [col for col in df.columns if col in dataset.columns and col != 'mal_id']
    df_clean = dataset.drop(columns=cols_to_drop+['image_url'])

    merged_df = pd.merge(df, df_clean, on='mal_id', how='inner')
    merged_df = merged_df.drop(columns=['Unnamed: 0'], errors='ignore')

    analysis_df = merged_df.copy()

    dataset = dataset.reset_index(drop=True)
    malid_to_index = dict(zip(dataset['mal_id'], dataset.index))

    # liked = hanya yang score > 7
    liked_indices = [
        malid_to_index[mid]
        for mid in analysis_df.loc[analysis_df['my_score'] > 7, 'mal_id']
        if mid in malid_to_index
    ]

    # watched = SEMUA yang pernah ditonton
    watched_indices = set(
        malid_to_index[mid]
        for mid in analysis_df['mal_id']
        if mid in malid_to_index
    )

    feature_matrix = build_feature_matrix_cached(dataset)

    response, scores = recommend_unwatched(
        dataset=dataset,
        feature_matrix=feature_matrix,
        liked_indices=liked_indices,   # my_score > 7
        watched_indices=watched_indices,
        top_n=25
    )

    result = response[['mal_id', 'title', 'link', 'episode', 'mal_score' , 'premiered', 'rank', 'similarity_score', 'image_url']].to_dict(orient='records')

    return result