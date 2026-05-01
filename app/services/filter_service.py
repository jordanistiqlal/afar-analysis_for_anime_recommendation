def filter_anime(data, selected_genres, selected_studios):
    from app.services.dataset_store import get_dataset, parse_list
    import pandas as pd
    import gc

    dataset = get_dataset()

    df = pd.DataFrame(data)
    df['mal_id'] = pd.to_numeric(df["id"], errors='coerce')

    # Drop kolom duplikat kecuali mal_id
    cols_to_drop = [c for c in df.columns if c in dataset.columns and c != 'mal_id']
    safe_drop = [c for c in cols_to_drop if c in dataset.columns]

    if 'image_url' in dataset.columns:
        safe_drop.append('image_url')

    df_clean = dataset.drop(columns=safe_drop, errors='ignore')

    merged_df = pd.merge(df, df_clean, on='mal_id', how='inner')

    if 'Unnamed: 0' in merged_df.columns:
        merged_df.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

    merged_df['genre'] = merged_df['genre'].apply(parse_list)
    merged_df['studio'] = merged_df['studio'].apply(parse_list)

    # FILTER LOGIC
    filtered_df = merged_df

    if selected_genres:
        filtered_df = filtered_df[
            filtered_df['genre'].apply(
                lambda genres: any(g in genres for g in selected_genres)
            )
        ]

    if selected_studios:
        filtered_df = filtered_df[
            filtered_df['studio'].apply(
                lambda studios: any(s in studios for s in selected_studios)
            )
        ]

    filtered_df = filtered_df.drop(columns=['mal_id'], errors='ignore')
    original_cols = [c for c in df.columns if c != 'mal_id']

    result = {
        "total_items": len(filtered_df),
        "data": filtered_df[original_cols].to_dict(orient='records')
    }
    
    del df, df_clean, merged_df, filtered_df, dataset
    gc.collect()

    return result