def recommend_unwatched(
    dataset,
    feature_matrix,
    liked_indices,
    watched_indices,
    top_n=10
):
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity
    from app.utils.features import build_user_profile

    # ---- COLD START ----
    if len(liked_indices) == 0:
        unwatched = dataset.drop(watched_indices)
        return unwatched.head(top_n), None

    # ---- USER PROFILE ----
    user_profile = build_user_profile(feature_matrix, liked_indices)
    if user_profile is None:
        return None, None

    # ---- SIMILARITY ----
    scores = cosine_similarity(user_profile, feature_matrix)[0]

    # ---- FILTER WATCHED ----
    scores[list(watched_indices)] = -1

    top_idx = np.argsort(scores)[-top_n:][::-1]

    results = dataset.iloc[top_idx].copy()
    results['similarity_score'] = scores[top_idx]

    return results, scores[top_idx]
