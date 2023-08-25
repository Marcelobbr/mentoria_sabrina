"""Responsible for executing all stages of the model.
"""

from guardian.logger import Logger
from argparse import ArgumentParser

from get_datasets import GetDatasets
from data_validation import DataValidation
from pbi_metrics import PbiMetrics
from predict import GetPredictions


def main():
    """Main function to execute the model stages.

    Notes:
    ------
    1 - Deixei as bases mesmo sem utilizá-las, por enquanto.
    """
    parser = ArgumentParser(
        description="Responsible for executing all stages of the model."
    )
    parser.add_argument(
        "--app_insight",
        dest="app_insight",
        type=str,
        required=False,
        help="Path to the config for the logger.",
    )
    args = parser.parse_args()

    logger = Logger.get_logger(__name__, cfg_path=args.app_insight)

    logger.info("Reading the datasets from Azure Data Lake.")
    get_datasets = GetDatasets()
    data_hot, data_impacto, data_perfil, data_tbogi = get_datasets.load_all_datasets()

    logger.info("Preparing the databases.")
    data_validation = DataValidation()
    data_perfil, data, data_ungrouped = data_validation.data_validation(
        data_hot, data_impacto, data_perfil, data_tbogi
    )

    logger.info("Generating metrics for Power BI.")
    pbi_metrics = PbiMetrics()
    datalake_conn_insight, path_name = pbi_metrics.calculate_pbi_metrics(
        data_perfil, data, data_ungrouped
    )

    logger.info("Generating predictions.")
    predict = GetPredictions()
    predict.make_predictions(data, datalake_conn_insight, path_name)


if __name__ == "__main__":
    main()
