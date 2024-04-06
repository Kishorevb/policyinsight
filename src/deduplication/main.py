from deduplication_configuration import ModelConf
from fetch_project_mappings import FetchProjectMappings
from fetch_articles_from_vanalyst import FetchArticlesFromvAnalyst
from generate_article_summaries import GenerateArticleSummaries
from cluster_article_summaries import ClusterArticleSummaries
from update_deduplicate_clusters import UpdateDeduplicatedClusters
import logging
import os


def execute_deduplication_flow(config):
    projects_mapper = FetchProjectMappings(config)
    projects_mapper_dict = projects_mapper.fetch_data()

    for project_id in projects_mapper_dict.keys():
        logging.info("Executing flow for the project id : %s", project_id)

        articles_fetcher = FetchArticlesFromvAnalyst(config)
        extracted_article_list = articles_fetcher.fetch_articles_from_vanalyst(project_id)

        if extracted_article_list:
            summary_generator = GenerateArticleSummaries(config)
            articles_list = summary_generator.generate_article_summaries(project_id, extracted_article_list)

            cluster_articles = ClusterArticleSummaries(config)
            articles_list = cluster_articles.cluster_article_summaries(project_id, articles_list)

            article_clusters_updater = UpdateDeduplicatedClusters(config)
            article_clusters_updater.update_deduplicate_clusters(project_id, articles_list,
                                                                 projects_mapper_dict[project_id])

        logging.info("Completed flow for the project id : %s", project_id)


if __name__ == '__main__':
    os.chdir("C:\\Users\\Gnowit\\repos\\deduplication\\")
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Deduplication flow started...")
    deduplication_config = ModelConf()
    execute_deduplication_flow(deduplication_config)
    logging.info("Deduplication flow has completed")
