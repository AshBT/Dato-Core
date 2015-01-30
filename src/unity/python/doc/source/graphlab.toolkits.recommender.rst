:mod:`recommender`
==========================

.. automodule:: graphlab.toolkits.recommender

.. currentmodule:: graphlab.recommender

creating a recommender
----------------------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  create

item similarity models
----------------------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  item_similarity_recommender.create
  item_similarity_recommender.get_default_options
  item_similarity_recommender.ItemSimilarityRecommender


factorization recommenders
--------------------------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  factorization_recommender.create
  factorization_recommender.get_default_options
  factorization_recommender.FactorizationRecommender

factorization recommenders for ranking
--------------------------------------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  ranking_factorization_recommender.create
  ranking_factorization_recommender.get_default_options
  ranking_factorization_recommender.RankingFactorizationRecommender

popularity-based recommenders
-----------------------------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  popularity_recommender.create
  popularity_recommender.get_default_options
  popularity_recommender.PopularityRecommender

utilities
---------
.. autosummary::
  :toctree: generated/
  :nosignatures:

  util.compare_models
  util.random_split_by_user
  util.precision_recall_by_user


