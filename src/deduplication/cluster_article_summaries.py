import logging
import tensorflow as tf
import tensorflow_text as text
import tensorflow_hub as hub
from sklearn.cluster import DBSCAN


class ClusterArticleSummaries:
    def __init__(self, config):
        logging.basicConfig(level=logging.DEBUG)
        with tf.device('/GPU:0'):
            self.embed = hub.load(config.embedding_model)

    def get_embeddings(self, texts):
        # Encode the texts into embeddings
        embeddings = self.embed(texts)
        return embeddings.numpy()

    def cluster_articles_dbscan(self, texts, eps=0.75, min_samples=1):
        # Get embeddings for the articles
        embeddings = self.get_embeddings(texts)

        # Initialize DBSCAN clustering
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)

        # Fit DBSCAN to the embeddings
        cluster_labels = dbscan.fit_predict(embeddings)

        return cluster_labels

    def cluster_article_summaries(self, project_id, articles_list):
        logging.info("Generating articles clusters for project id: %s", project_id)

        article_texts = [article['Summary'] for article in articles_list]
        cluster_labels = self.cluster_articles_dbscan(article_texts)
        for i, label in enumerate(cluster_labels):
            articles_list[i].update({'Cluster': label})

        logging.info(
            f"Number of articles filtered from clustering for project_id: {project_id} is : {len(articles_list) - len(set(cluster_labels))} of {len(articles_list)} articles")
        return articles_list
