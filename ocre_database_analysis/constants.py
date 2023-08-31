from pathlib import Path


ROOT_FOLDER = Path(__file__).resolve().parents[1]
APP_FOLDER = ROOT_FOLDER / "ocre_database_analysis"

DATA_FOLDER = ROOT_FOLDER / "data"
SAMPLE_FOLDER = DATA_FOLDER / "sample_html"
EXPLORATORY_FOLDER = DATA_FOLDER / "exploratory_data"

IMAGES_FOLDER = ROOT_FOLDER / "images"
COIN_FOLDER = IMAGES_FOLDER / "coins"

SQL_FOLDER = ROOT_FOLDER / "sql"

OCRE_HOME_PAGE = "https://numismatics.org/ocre/"
OCRE_BROWSE_PAGE = (
    "https://numismatics.org/ocre/results?q=objectType_facet%3A%22Coin%22&start="
)


if __name__ == "__main__":
    pass
